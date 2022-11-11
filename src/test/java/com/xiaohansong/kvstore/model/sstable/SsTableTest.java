package com.xiaohansong.kvstore.model.sstable;

import com.xiaohansong.kvstore.model.command.Command;
import com.xiaohansong.kvstore.model.command.RmCommand;
import com.xiaohansong.kvstore.model.command.SetCommand;
import org.junit.Test;

import java.util.TreeMap;

public class SsTableTest {

    private static final String TEST_PATH = "test_data/test.txt";

    @Test
    public void createFromIndex() {
        TreeMap<String, Command> memory = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            SetCommand setCommand = new SetCommand("key" + i, "value" + i);
            memory.put(setCommand.getKey(), setCommand);
        }
        memory.put("key100", new SetCommand("key100", "value100"));
        memory.put("key100", new RmCommand("key100"));

        SsTable ssTable = SsTable.createFromMemoryTable(TEST_PATH, 3, memory);
    }

    @Test
    public void query() {
        // 每次打开文件后，构建内存中的稀疏索引表
        SsTable ssTable = SsTable.createFromFile(TEST_PATH);
        System.out.println(ssTable.query("key0"));
        System.out.println(ssTable.query("key6"));
        System.out.println(ssTable.query("key9"));
        System.out.println(ssTable.query("key100"));
    }
}