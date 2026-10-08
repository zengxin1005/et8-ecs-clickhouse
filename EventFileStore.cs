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
        
        private long _lastSaveMs;
        private const int SAVE_INTERVAL_MS = 15 * 1000;   // 每 15 秒保存一次
        private const uint FILE_MAGIC = 0x4B41464B;
        private const string INDEX_FILE = "events.idx";
        private const string SEGMENT_PREFIX = "event_";
        private const string SEGMENT_EXT = ".dat";
        private const int BUFFER_SIZE = 64 * 1024;   //如果马上flush就无效
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
            _lastSaveMs = TimeInfo.Instance.ClientNow();
            
            if (!Directory.Exists(_storePath))
                Directory.CreateDirectory(_storePath);
            
            LoadIndex();
            
            _config.Log($"初始化完成: 事件数={_index.Count}, 当前segment={_currentSegmentId}");
        }
        
        #region 写入操作 - 字节数据
        
        private bool StoreEvent(string eventId, byte[] data)
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(EventFileStore));
                
            if (string.IsNullOrEmpty(eventId))
                throw new ArgumentException("Event ID cannot be null or empty", nameof(eventId));
                
            if (data == null || data.Length == 0)
                throw new ArgumentException("Data cannot be null or empty");
            
            var dataLength = data.Length;
            var eventIdBytes = Encoding.UTF8.GetBytes(eventId);

            if (eventIdBytes.Length > MAX_EVENTID_LENGTH)
            {
                throw new ArgumentException($"Event ID too long: {eventIdBytes.Length} > {MAX_EVENTID_LENGTH}");
            }
            if (dataLength > SEGMENTSIZE)
            {
                throw new ArgumentException($"Event data too large: {dataLength} > {SEGMENTSIZE}");
            }

            if (_index.ContainsKey(eventId))
            {
                throw new ArgumentException($"Event ID duplicate: {eventId}");
            }

            try
            {
                var crc32 = CalculateCrc32(data);

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
                
                _activeWriter.Flush();//每次直接刷OS BUFFER_SIZE 就没有意义 
                //_activeStream.Flush(); //_activeWriter.Flush()还会多刷自己缓冲，一般没有缓冲，   开启 Asynchronous  用_activeStream.FlushAsync()更好

                _currentSegmentSize += totalSize;

                _index[eventId] = new EventIndex
                {
                    SegmentId = _currentSegmentId, FileOffset = fileOffset, DataLength = dataLength, Crc32 = crc32
                };
                
                if (TimeInfo.Instance.ClientNow() - _lastSaveMs >= SAVE_INTERVAL_MS)
                {
                    SaveIndex();//一定次数保存一次可能会丢，但影响不大，可以降低磁盘IO
                    _lastSaveMs = TimeInfo.Instance.ClientNow();
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
        
        public bool StoreEventString(string eventId, string data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            var bytes = Encoding.UTF8.GetBytes(data);
            return StoreEvent(eventId, bytes);
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
        
        public void DeleteEvent(string eventId)
        {
            if (string.IsNullOrEmpty(eventId))
                return ;
            
            if (_index.Remove(eventId, out _))
            {
                if (TimeInfo.Instance.ClientNow() - _lastSaveMs >= SAVE_INTERVAL_MS)
                {
                    SaveIndex();//一定次数保存一次可能会丢，但影响不大，可以降低磁盘IO
                    _lastSaveMs = TimeInfo.Instance.ClientNow();
                }
            }
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
        
        public Queue<string> GetAllEventIdsAsQueue(int maxCount)
        {
            if (maxCount <= 0) return new Queue<string>();

            var ordered = _index
                .OrderBy(kvp => kvp.Value.SegmentId)
                .ThenBy(kvp => kvp.Value.FileOffset);

            // 取不满时也要按写入序截取最早的 maxCount 条
            return new Queue<string>(ordered.Take(maxCount).Select(kvp => kvp.Key));
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
                    
                    writer.Flush();//每次直接刷OS BUFFER_SIZE 就没有意义 
                    //fs.Flush();//writer.Flush()还会多刷自己缓冲，一般没有缓冲， 开启 Asynchronous  fs.FlushAsync()更好
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
            {
                RebuildIndexFromSegments();
                return;
            }
            
            // 索引写到一半被杀就会留下"半截索引"：头部说 count 条，实际只落了前几条。
            int count = 0;      // 头部声明的总条数
            int loaded = 0;     // 实际完整读出的条数
            
            try
            {
                using var fs = new FileStream(indexFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,BUFFER_SIZE,FileOptions.SequentialScan);
                using var reader = new BinaryReader(fs, Encoding.UTF8, true);
                
                count = reader.ReadInt32();
                
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
                        
                        loaded++;
                        
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
                Log.Error($"[DRSDK] 索引损坏，改为从 segment 重建");
                RebuildIndexFromSegments();
                return;
            }
            
            // 半截索引：_index 只覆盖前段，看着"正常"（非空、不报错），但索引没提到的那些 segment
            if (count < 0 || loaded < count)
            {
                Log.Error($"[DRSDK] 索引不完整: 只读出 {loaded}/{count} 条，改为从 segment 重建");
                RebuildIndexFromSegments();
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
                _config.Log($"优雅关闭刷盘成功");
            }
            catch
            {
                // 忽略释放错误
                Log.Error($"[DRSDK] 优雅关闭刷盘失败");
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
                    if (segmentId < 0)
                    {
                        _config.Log($"跳过非段文件名: {Path.GetFileName(file)}");
                        continue;
                    }
                    
                    if (segmentId == _currentSegmentId)
                        continue;
                    
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
            if (!fileName.StartsWith(SEGMENT_PREFIX, StringComparison.Ordinal))
            {
                return -1;
            }
            var idStr = fileName.Substring(SEGMENT_PREFIX.Length);
            return int.TryParse(idStr, out var id) ? id : -1;
        }

        
        
        private static long NextOffset(long current, long declaredSkip, long fileLength)
        {
            return declaredSkip > 4 && current + declaredSkip <= fileLength ? current + declaredSkip : current + 4;
        }
        /// <summary>
        /// 从所有 segment 文件重建索引
        /// </summary>
        private void RebuildIndexFromSegments()
        {
            //重建有可能导致相同Event重发，但是总比丢的好
            var segmentFiles = Directory.GetFiles(_storePath, $"{SEGMENT_PREFIX}*{SEGMENT_EXT}")
                .OrderBy(f => f)  // 按文件名排序
                .ToList();
            if (segmentFiles.Count == 0)
            {
                _index.Clear();
                _config.Log($"没有找到任何 segment 文件，索引已清空");
                return;
            }

            var newIndex = new Dictionary<string, EventIndex>();
            
            // 段号以「盘上实际文件名」为准，而不是只看索引条目：
            // 尾部可能存在不含有效事件的空段。但"名字像段、内容不是段"的文件不算（见下面 hasRecord）。
            int maxSegmentId = 0;
            
            foreach (var segmentFile in segmentFiles)
            {
                var segmentId = ParseSegmentId(segmentFile);
                if (segmentId < 0)
                {
                    _config.Log($"跳过非段文件名: {Path.GetFileName(segmentFile)}");
                    continue;
                }
                
                using var fs = new FileStream(segmentFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, BUFFER_SIZE,
                    FileOptions.SequentialScan);
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
                            offset = NextOffset(offset, 4L + 4 + eventIdLength + 4 + 4, fs.Length);
                            continue;
                        }
                        
                        var eventIdBytes = reader.ReadBytes(eventIdLength);
                        var eventId = Encoding.UTF8.GetString(eventIdBytes);
                        
                        // 读取数据长度
                        var dataLength = reader.ReadInt32();
                        if (dataLength <= 0 || dataLength > SEGMENTSIZE)
                        {
                            offset = NextOffset(offset, 4L + 4 + eventIdLength + 4 + 4 + dataLength + 4, fs.Length);
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
                if (segmentId > maxSegmentId)
                {
                    maxSegmentId = segmentId;
                }
            }
            
            // 替换索引
            _index.Clear();
            foreach (var kvp in newIndex)
            {
                _index[kvp.Key] = kvp.Value;
            }
            
            // 重建后必须把段号接上：否则继续写入会落回旧段号，
            // 段满换段时 CreateNewSegment 会用 FileMode.Create 把已存在的段文件截断。
            if (maxSegmentId > _currentSegmentId)
            {
                _currentSegmentId = maxSegmentId;
            }
            
            // 保存重建后的索引
            SaveIndex();
            _config.Log($"索引重建完成，共 {_index.Count} 个事件，当前段号={_currentSegmentId}");
        }
        
        #endregion
    }
}