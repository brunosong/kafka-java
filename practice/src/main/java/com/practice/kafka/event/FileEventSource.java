package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FileEventSource implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    public boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer = 0; //현재 어디까지 읽었냐
    private EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {

        try {

            while (this.keepRunning) {

                try {
                    Thread.sleep(this.updateInterval);

                    //file에 크기를 계산한다. (파일에 크기가 커진다면 추가가 된것이다)
                    long len = this.file.length();

                    if(len < this.filePointer) {
                        logger.info("file was reset as filePointer is logger than file length");
                        filePointer = len;
                    } else if(len > this.filePointer) {
                        readAppendAndSend();
                    } else {
                        continue;
                    }

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
        randomAccessFile.seek(this.filePointer); //여기서 부터 읽어 들이겠다.

        String line = null;
        while ((line = randomAccessFile.readLine()) != null) {
            sendMessage(line);
        }

        //파일이 변경되었으니깐 file의 filePointer를 현재 file의 마지막으로 재 설정함
        this.filePointer = randomAccessFile.getFilePointer();

    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {

        String[] tokens = line.split(",");
        String key = tokens[0];
        String value = Arrays.stream(tokens).skip(1)
                .collect(Collectors.joining(","));

        MessageEvent messageEvent = new MessageEvent(key, value);
        eventHandler.onMessage(messageEvent);
    }
}
