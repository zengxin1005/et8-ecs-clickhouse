/*
 * 文件名: EventFileStore.cs
 * 作者: zengxin
 * 创建日期: 2026-2-9
 */
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ET.DrSdk
{
    [EnableClass]
    public class EventFileStore : IDisposable
    {
        private readonly string _storePath;

        
        private FileStream _activeStream;
        private BinaryWriter _activeWriter;
        private int _currentSegmentId;
        private long _currentSegmentSize;
        
        private readonly Dictionary<string, EventIndex> _index = new();
        private bool _isDisposed;
        private DateTime _lastSaveTime = DateTime.Now;
        private const int SAVE_INTERVAL_SECONDS = 30;   // 每 30 秒保存一次
        private const uint FILE_MAGIC = 0x4B41464B;
        private const string INDEX_FILE = "events.idx";
        private const string SEGMENT_PREFIX = "event_";
        private const string SEGMENT_EXT = ".dat";
        private const int BUFFER_SIZE = 512 * 1024;   
        private const int MAX_EVENTID_LENGTH = 1024;
        private const int SEGMENTSIZE = 10 * 1024 * 1024;
        private DRSDKConfig _config;
        private struct EventIndex
        {
            public int SegmentId;
            public long FileOffset;
            public int DataLength;
            public uint Crc32;
        }
        
        public EventFileStore(string storePath, DRSDKConfig config)
        {
            if (string.IsNullOrEmpty(storePath))
                throw new ArgumentException("Store path cannot be null or empty", nameof(storePath));
                
            _storePath = storePath;
            _config = config;
            
            if (!Directory.Exists(_storePath))
                Directory.CreateDirectory(_storePath);
            
            LoadIndex();
            
            _config.Log($"初始化完成: 事件数={_index.Count}, 当前segment={_currentSegmentId}");
        }
        
        #region 写入操作 - 字节数据
        
        private bool StoreEvent(string eventId, byte[] data,bool saveIndex = true)
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(EventFileStore));
                
            if (string.IsNullOrEmpty(eventId))
                throw new ArgumentException("Event ID cannot be null or empty", nameof(eventId));
                
            if (data == null || data.Length == 0)
                throw new ArgumentException("Data cannot be null or empty");
            
            try
            {
                var dataLength = data.Length;
                var crc32 = CalculateCrc32(data);
                var eventIdBytes = Encoding.UTF8.GetBytes(eventId);

                if (eventIdBytes.Length > MAX_EVENTID_LENGTH)
                {
                    throw new ArgumentException($"Event ID too long: {eventIdBytes.Length} > {MAX_EVENTID_LENGTH}");
                }
                if (_activeWriter == null)
                {
                    EnsureActiveSegmentWriter();
                }

                var headerSize = 4 + 4 + eventIdBytes.Length + 4;
                var totalSize = headerSize + dataLength + 4;

                if (_currentSegmentSize + totalSize > SEGMENTSIZE)
                {
                    CreateNewSegment();
                }

                var fileOffset = _currentSegmentSize;

                _activeWriter.Write(FILE_MAGIC);
                _activeWriter.Write(eventIdBytes.Length);
                _activeWriter.Write(eventIdBytes);
                _activeWriter.Write(dataLength);
                _activeWriter.Write(data);
                _activeWriter.Write(crc32);

                _currentSegmentSize += totalSize;

                _index[eventId] = new EventIndex
                {
                    SegmentId = _currentSegmentId, FileOffset = fileOffset, DataLength = dataLength, Crc32 = crc32
                };
                _activeWriter.Flush();
                //_activeStream.Flush(); //当然如果有刷的话 开启 Asynchronous  用_activeStream.FlushAsync()更好
                // 没有马上刷文件，待文件缓冲区满后才刷到OS页，有很小的概率会丢,但性能好
                
                if (saveIndex && (DateTime.Now - _lastSaveTime).TotalSeconds >= SAVE_INTERVAL_SECONDS)
                {
                    SaveIndex();//一定次数保存一次可能会丢，但影响不大，可以降低磁盘IO
                    _lastSaveTime = DateTime.Now;
                }
                return true;
          
            }
            catch (Exception ex)
            {
                Log.Error($"[DRSDK] 存储事件失败: eventId={eventId}, error={ex.Message}");
                return false;
            }
        }
        
        #endregion
        
        #region 写入操作 - 字符串数据
        
        public bool StoreEventString(string eventId, string data,bool saveIndex = true)
        {
            var bytes = Encoding.UTF8.GetBytes(data);
            return StoreEvent(eventId, bytes,saveIndex);
        }
        #endregion
        
        #region 读取操作 - 字节数据
        
        private byte[] ReadEvent(string eventId)
        {
            if (string.IsNullOrEmpty(eventId))
                return null;
            
            if (!_index.TryGetValue(eventId, out EventIndex index))
            {
                _config.Log($"事件不存在于索引: {eventId}");
                return null;
            }
            
            try
            {
                var segmentFile = GetSegmentFilePath(index.SegmentId);
                
                if (!File.Exists(segmentFile))
                {
                    Log.Error($"[DRSDK] Segment文件不存在: {segmentFile}");
                    return null;
                }
                
                using var fs = new FileStream(segmentFile, FileMode.Open, 
                    FileAccess.Read, FileShare.ReadWrite, MAX_EVENTID_LENGTH, 
                    FileOptions.RandomAccess);
                
                var fileSize = fs.Length;
                
                if (index.FileOffset >= fileSize)
                {
                    Log.Error($"[DRSDK] 偏移超出范围: offset={index.FileOffset}, fileSize={fileSize}");
                    return null;
                }
                
                fs.Seek(index.FileOffset, SeekOrigin.Begin);
                
                using var reader = new BinaryReader(fs, Encoding.UTF8, true);
                
                if (reader.ReadUInt32() != FILE_MAGIC)
                {
                    Log.Error($"[DRSDK] 魔数不匹配");
                    return null;
                }
                
                var eventIdLength = reader.ReadInt32();
                if (eventIdLength <= 0 || eventIdLength > MAX_EVENTID_LENGTH)
                {
                    Log.Error($"[DRSDK] 事件ID长度异常: {eventIdLength}");
                    return null;
                }
                
                var eventIdBytes = reader.ReadBytes(eventIdLength);
                var storedEventId = Encoding.UTF8.GetString(eventIdBytes);
                
                if (storedEventId != eventId)
                {
                    Log.Error($"[DRSDK] 事件ID不匹配: expected={eventId}, actual={storedEventId}");
                    return null;
                }
                
                var length = reader.ReadInt32();
                if (length != index.DataLength)
                {
                    Log.Error($"[DRSDK] 数据长度不匹配: expected={index.DataLength}, actual={length}");
                    return null;
                }
                
                var data = reader.ReadBytes(length);
                
                var storedCrc32 = reader.ReadUInt32();
                var calculatedCrc32 = CalculateCrc32(data);
                
                if (storedCrc32 != calculatedCrc32)
                {
                    Log.Error($"[DRSDK] CRC32校验失败: eventId={eventId}");
                    return null;
                }
                return data;
            }
            catch (Exception ex)
            {
                Log.Error($"[DRSDK] 读取事件失败: eventId={eventId}, error={ex.Message}");
                return null;
            }
        }
        

        
        #endregion
        
        #region 读取操作 - 字符串数据
        
        public string ReadEventString(string eventId)
        {
            var bytes = ReadEvent(eventId);
            return bytes != null ? Encoding.UTF8.GetString(bytes) : null;
        }
        

        
        #endregion
        
        
        #region 删除操作
        
        public bool DeleteEvent(string eventId,bool saveIndex = true)
        {
            if (string.IsNullOrEmpty(eventId))
                return false;
            
            if (_index.Remove(eventId, out _))
            {
                if (saveIndex && (DateTime.Now - _lastSaveTime).TotalSeconds >= SAVE_INTERVAL_SECONDS)
                {
                    SaveIndex();//一定次数保存一次可能会丢，但影响不大，可以降低磁盘IO
                    _lastSaveTime = DateTime.Now;
                }
                return true;
            }
            return false;
        }
        
        
        #endregion
        
        #region 查询操作
        
        public bool ContainsEvent(string eventId)
        {
            return _index.ContainsKey(eventId);
        }
        
        public List<string> GetAllEventIds()
        {
            return _index.Keys.ToList();
        }
        
        public Queue<string> GetAllEventIdsAsQueue()
        {
            // 按文件偏移量排序（写入顺序）
            var sortedEventIds = _index
                .OrderBy(kvp => kvp.Value.SegmentId)
                .ThenBy(kvp => kvp.Value.FileOffset)
                .Select(kvp => kvp.Key)
                .ToList();
            
            return new Queue<string>(sortedEventIds);
        }
        
        public int GetEventCount()
        {
            return _index.Count;
        }
        
        #endregion
        
        #region 管理操作
        
 
        
        private void SaveIndex()
        {
            try
            {
                var indexFile = Path.Combine(_storePath, INDEX_FILE);
                using (var fs = new FileStream(indexFile, FileMode.Create, FileAccess.Write, FileShare.ReadWrite, BUFFER_SIZE, 
                           FileOptions.SequentialScan))
                using (var writer = new BinaryWriter(fs, Encoding.UTF8, true))
                {
                    writer.Write(_index.Count);
                    
                    foreach (var kvp in _index)
                    {
                        writer.Write(kvp.Key);
                        writer.Write(kvp.Value.SegmentId);
                        writer.Write(kvp.Value.FileOffset);
                        writer.Write(kvp.Value.DataLength);
                        writer.Write(kvp.Value.Crc32);
                    }
                    
                    writer.Flush();
                    //fs.Flush();//当然如果有刷的话 开启 Asynchronous  fs.FlushAsync()更好
                    // 没有马上刷文件，待文件缓冲区满后才刷到OS页，有很小的概率会丢,但性能好
                }
                _config.Log($"索引保存成功");
            }
          	catch (Exception ex)
            {
                Log.Error($"[DRSDK] 索引保存失败, error={ex.Message}");
            }
        }
        
     
        
        #endregion
        
        #region 私有方法
        
        private string GetSegmentFilePath(int segmentId)
        {
            return Path.Combine(_storePath, $"{SEGMENT_PREFIX}{segmentId:000000}{SEGMENT_EXT}");
        }
        
        private void EnsureActiveSegmentWriter()
        {
   
            if (_activeWriter == null)
            {
                var filePath = GetSegmentFilePath(_currentSegmentId);
                
                _activeStream = new FileStream(filePath, FileMode.OpenOrCreate, 
                    FileAccess.Write, FileShare.ReadWrite, BUFFER_SIZE,
                    FileOptions.SequentialScan);
                
                _activeWriter = new BinaryWriter(_activeStream, Encoding.UTF8, true);
                
                if (_activeStream.Length > 0)
                {
                    _activeStream.Seek(0, SeekOrigin.End);
                }
                
                _currentSegmentSize = _activeStream.Length;
                _config.Log($"打开写入器: {filePath}, 当前大小={_currentSegmentSize}");
            }
        }
        
        private void CreateNewSegment()
        {
            _activeWriter?.Dispose();
            _activeWriter = null;
            _activeStream?.Dispose();
            _activeStream = null;
            
            _currentSegmentId++;
            
            var filePath = GetSegmentFilePath(_currentSegmentId);
            _activeStream = new FileStream(filePath, FileMode.Create, 
                FileAccess.Write, FileShare.ReadWrite, BUFFER_SIZE,
                FileOptions.SequentialScan);
            
            _activeWriter = new BinaryWriter(_activeStream, Encoding.UTF8, true);
            _currentSegmentSize = 0;
            
            _config.Log($"创建新segment: {_currentSegmentId}");
        }
        
        private void LoadIndex()
        {
            var indexFile = Path.Combine(_storePath, INDEX_FILE);
            
            if (!File.Exists(indexFile))
                return;
            
            try
            {
                using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,BUFFER_SIZE,FileOptions.SequentialScan);
                using var reader = new BinaryReader(fs, Encoding.UTF8, true);
                
                var count = reader.ReadInt32();
                
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var eventId = reader.ReadString();
                        var index = new EventIndex
                        {
                            SegmentId = reader.ReadInt32(),
                            FileOffset = reader.ReadInt64(),
                            DataLength = reader.ReadInt32(),
                            Crc32 = reader.ReadUInt32()
                        };
                        
                        if (index.SegmentId >= 0 && index.FileOffset >= 0 && 
                            index.DataLength > 0 && index.DataLength <= SEGMENTSIZE)
                        {
                            _index[eventId] = index;
                            
                            if (index.SegmentId > _currentSegmentId)
                            {
                                _currentSegmentId = index.SegmentId;
                            }
                        }
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                }
            }
            catch
            {
                // 索引文件损坏，忽略
            }
        }
        

        
        private uint CalculateCrc32(byte[] data)
        {
            const uint polynomial = 0xEDB88320;
            uint crc = 0xFFFFFFFF;
            
            foreach (byte b in data)
            {
                crc ^= b;
                for (int i = 0; i < 8; i++)
                {
                    if ((crc & 1) != 0)
                        crc = (crc >> 1) ^ polynomial;
                    else
                        crc >>= 1;
                }
            }
            
            return ~crc;
        }
        
        #endregion
        
        #region IDisposable实现
        
        public void Dispose()
        {
            if (_isDisposed)
                return;
                
            _isDisposed = true;
            
            try
            {
                SaveIndex();
                _activeWriter?.Dispose();
                _activeWriter = null;
                _activeStream?.Dispose();
                _activeStream = null;
                _config.Log($"优雅关闭，刷盘");
            }
            catch
            {
                // 忽略释放错误
            }
        }
        
        
        
        public void CleanEmptySegments()
        {
            try
            {
                var activeSegmentIds = _index.Values.Select(idx => idx.SegmentId).Distinct().ToHashSet();

                string[] files = Directory.GetFiles(_storePath, $"{SEGMENT_PREFIX}*{SEGMENT_EXT}");
                foreach (var file in files)
                {
                    var segmentId = ParseSegmentId(file);
                    if (segmentId == _currentSegmentId)
                    {
                        continue;
                    }
                    if (!activeSegmentIds.Contains(segmentId))
                    {
                        File.Delete(file);
                        _config.Log($"清理空 segment: {Path.GetFileName(file)}");
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"[DRSDK] 清理空 segment 失败: {ex.Message}");
            }
        }

        private int ParseSegmentId(string filePath)
        {
            var fileName = Path.GetFileNameWithoutExtension(filePath);
            var idStr = fileName.Replace(SEGMENT_PREFIX, "");
            return int.Parse(idStr);
        }
        
        
        /// <summary>
        /// 从所有 segment 文件重建索引
        /// </summary>
        public void RebuildIndexFromSegments()
        {
            var newIndex = new Dictionary<string, EventIndex>();
            var segmentFiles = Directory.GetFiles(_storePath, $"{SEGMENT_PREFIX}*{SEGMENT_EXT}")
                .OrderBy(f => f)  // 按文件名排序
                .ToList();
            
            foreach (var segmentFile in segmentFiles)
            {
                var segmentId = ParseSegmentId(segmentFile);
                
                using var fs = new FileStream(segmentFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new BinaryReader(fs, Encoding.UTF8, true);
                
                long offset = 0;
                while (offset < fs.Length)
                {
                    fs.Seek(offset, SeekOrigin.Begin);
                    
                    try
                    {
                        // 读取魔数
                        if (reader.ReadUInt32() != FILE_MAGIC)
                        {
                            _config.Log($"魔数不匹配，跳过偏移 {offset}");
                            offset += 4;
                            continue;
                        }
                        
                        // 读取事件ID长度和内容
                        var eventIdLength = reader.ReadInt32();
                        if (eventIdLength <= 0 || eventIdLength > MAX_EVENTID_LENGTH)
                        {
                            offset += 4 + 4 + eventIdLength + 4 + 4;
                            continue;
                        }
                        
                        var eventIdBytes = reader.ReadBytes(eventIdLength);
                        var eventId = Encoding.UTF8.GetString(eventIdBytes);
                        
                        // 读取数据长度
                        var dataLength = reader.ReadInt32();
                        if (dataLength <= 0 || dataLength > SEGMENTSIZE)
                        {
                            offset += 4 + 4 + eventIdLength + 4 + 4 + dataLength + 4;
                            continue;
                        }
                        
                        // 读取数据
                        var data = reader.ReadBytes(dataLength);
                        
                        // 读取 CRC
                        var crc32 = reader.ReadUInt32();
                        
                        // 计算总大小
                        var totalSize = 4 + 4 + eventIdLength + 4 + dataLength + 4;
                        
                        newIndex[eventId] = new EventIndex
                        {
                            SegmentId = segmentId,
                            FileOffset = offset,
                            DataLength = dataLength,
                            Crc32 = crc32
                        };
                        
                        offset += totalSize;
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _config.Log($"读取事件失败: {ex.Message}");
                        offset += 4;  // 跳过损坏部分
                    }
                }
            }
            
            // 替换索引
            _index.Clear();
            foreach (var kvp in newIndex)
            {
                _index[kvp.Key] = kvp.Value;
            }
            
            // 保存重建后的索引
            SaveIndex();
            _config.Log($"索引重建完成，共 {_index.Count} 个事件");
        }
        
        #endregion
    }
}