package com.xiaohansong.kvstore.model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.xiaohansong.kvstore.model.Position;
import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.RmCommand;
import com.xiaohansong.kvstore.model.command.SetCommand;
import com.xiaohansong.kvstore.utils.ConvertUtil;
import com.xiaohansong.kvstore.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 排序字符串表
 */
public class SsTable implements Closeable {

    public static final String RW = "rw";

    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    /**
     * 表索引信息
     */
    private TableMetaInfo tableMetaInfo;

    /**
     * 字段稀疏索引
     */
    private TreeMap<String, Position> sparseIndex;

    /**
     * 文件句柄
     */
    private final RandomAccessFile tableFile;

    /**
     * 文件路径
     */
    private final String filePath;

    /**
     * @param filePath 表文件路径
     * @param partSize 数据分区大小
     */
    private SsTable(String filePath, int partSize) {
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setPartSize(partSize);
        this.filePath = filePath;
        try {
            this.tableFile = new RandomAccessFile(filePath, RW);
            // 设置 当前文件引用的 指针
            tableFile.seek(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        sparseIndex = new TreeMap<>();
    }

    /**
     * 从内存表中构建ssTable
     *
     * @param filePath
     * @param partSize
     * @param memoryTable
     * @return
     */
    public static SsTable createFromMemoryTable(String filePath, int partSize, TreeMap<String, Command> memoryTable) {
        SsTable ssTable = new SsTable(filePath, partSize);
        ssTable.initFromIndex(memoryTable);
        return ssTable;
    }

    /**
     * 从文件中构建ssTable
     *
     * @param filePath
     * @return
     */
    public static SsTable createFromFile(String filePath) {
        SsTable ssTable = new SsTable(filePath, 0);
        // 构建内存中的稀疏索引表
        ssTable.restoreFromFile();
        return ssTable;
    }

    /**
     * 从ssTable中查询数据
     *
     * @param key
     * @return
     */
    public Command query(String key) {
        try {
            LinkedList<Position> sparseKeyPositionList = new LinkedList<>();

            Position lastSmallPosition = null;
            Position firstBigPosition = null;

            //从稀疏索引中找到最后一个小于key的位置，以及第一个大于key的位置(这个没有必要吧)
            for (String k : sparseIndex.keySet()) {
                // 这个有可能把 等于的 也计算在内的了
                if (k.compareTo(key) <= 0) {
                    lastSmallPosition = sparseIndex.get(k);
                } else {
                    firstBigPosition = sparseIndex.get(k);
                    break;
                }
            }
            if (lastSmallPosition != null) {
                sparseKeyPositionList.add(lastSmallPosition);
            }
            if (firstBigPosition != null) {
                sparseKeyPositionList.add(firstBigPosition);
            }
            if (sparseKeyPositionList.size() == 0) {
                return null;
            }

            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseKeyPositionList]: {}", sparseKeyPositionList);

            Position firstKeyPosition = sparseKeyPositionList.getFirst();
            Position lastKeyPosition = sparseKeyPositionList.getLast();
            long start = 0;
            long len = 0;
            start = firstKeyPosition.getStart();

            // 有可能 等于吗？
            if (firstKeyPosition.equals(lastKeyPosition)) {
                len = firstKeyPosition.getLen();
            } else {
                // 两个 段之间的 总长度
                len = lastKeyPosition.getStart() + lastKeyPosition.getLen() - start;
            }

            //key如果存在，必定位于区间内，所以只需要读取区间内的数据，减少io
            byte[] dataPart = new byte[(int) len];
            tableFile.seek(start);
            // 全都读取到 内存 数组中
            tableFile.read(dataPart);
            int pStart = 0;
            //读取分区数据
            for (Position position : sparseKeyPositionList) {
                // 截取第一个 段
                JSONObject dataPartJson = JSONObject.parseObject(new String(dataPart, pStart, (int) position.getLen()));
                LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][dataPartJson]: {}", dataPartJson);
                if (dataPartJson.containsKey(key)) {
                    JSONObject value = dataPartJson.getJSONObject(key);
                    return ConvertUtil.jsonToCommand(value);
                }
                // 下一个 段
                pStart = pStart + (int) position.getLen();
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 从文件中恢复ssTable到内存中
     * 只加载 SSTable 稀疏索引到内存中
     */
    private void restoreFromFile() {
        try {
            // 从文件中 读取元数据
            TableMetaInfo tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][tableMetaInfo]: {}", tableMetaInfo);
            // 读取稀疏索引
            byte[] indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
            // 稀疏索引区 首地址
            tableFile.seek(tableMetaInfo.getIndexStart());
            // 稀疏索引数据
            tableFile.read(indexBytes);
            String indexStr = new String(indexBytes, StandardCharsets.UTF_8);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][indexStr]: {}", indexStr);
            // 构建内存中的稀疏索引表
            sparseIndex = JSONObject.parseObject(indexStr, new TypeReference<TreeMap<String, Position>>() {
            });
            this.tableMetaInfo = tableMetaInfo;
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseIndex]: {}", sparseIndex);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 从内存表转化为ssTable
     *
     * @param memory
     */
    private void initFromIndex(TreeMap<String, Command> memory) {
        try {
            JSONObject partData = new JSONObject(true);
            // 获得当前文件写指针位置
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Command command : memory.values()) {
                //处理set命令
                if (command instanceof SetCommand) {
                    SetCommand set = (SetCommand) command;
                    partData.put(set.getKey(), set);
                }
                //处理rm命令
                if (command instanceof RmCommand) {
                    RmCommand rm = (RmCommand) command;
                    partData.put(rm.getKey(), rm);
                }

                //达到分段数量，开始写入数据段，目的是利用段 构建 稀疏索引
                if (partData.size() >= tableMetaInfo.getPartSize()) {
                    writeDataPart(partData);
                }
            }

            //遍历完之后如果有剩余的数据（尾部数据不一定达到分段大小条件）也写入文件
            if (partData.size() > 0) {
                writeDataPart(partData);
            }
            // 总体数据长度
            long dataLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
            tableMetaInfo.setDataLen(dataLen);
            //保存稀疏索引
            byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            // SSTable 写入 稀疏索引
            tableFile.write(indexBytes);
            tableMetaInfo.setIndexLen(indexBytes.length);
            LoggerUtil.debug(LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

            //SSTable 写入 文件元数据
            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info(LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 将数据分区写入文件
     *
     * @param partData
     * @throws IOException
     */
    private void writeDataPart(JSONObject partData) throws IOException {
        byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
        long start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        //记录数据段的第一个key到稀疏索引中
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(s, start, partDataBytes.length)));
        partData.clear();
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }
}
