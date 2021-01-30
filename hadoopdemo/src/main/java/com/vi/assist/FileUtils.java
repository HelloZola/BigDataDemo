package com.vi.assist;

import com.vi.utils.DateUtils;
import com.vi.utils.RandomUtils;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;

public class FileUtils {

    @Test
    public void writeFile() {
        try {
            File file = new File("E:\\tmp\\test1.txt");
            int count = 10;
            FileWriter fw = new FileWriter(file, false);
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i < count; i++) {
                String content = "张三" + RandomUtils.getRandomNum(300) + ","
                        + DateUtils.dateToString(DateUtils.datePlus(new Date(), 1), DateUtils.pattern1);
                bw.write(content);
                bw.newLine();
            }
            bw.close();
            fw.close();
            System.out.println("test1 done!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
