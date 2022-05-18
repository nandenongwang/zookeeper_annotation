package org.apache.zookeeper.server.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件填充扩大工具类
 */
public class FilePadding {

    private static final Logger LOG;

    /**
     * 默认64M
     */
    private static long preAllocSize = 64 * 1024 * 1024;

    /**
     * 扩大文件填充用、无意义
     */
    private static final ByteBuffer fill = ByteBuffer.allocateDirect(1);

    static {
        LOG = LoggerFactory.getLogger(FileTxnLog.class);

        String size = System.getProperty("zookeeper.preAllocSize");
        if (size != null) {
            try {
                preAllocSize = Long.parseLong(size) * 1024;
            } catch (NumberFormatException e) {
                LOG.warn("{} is not a valid value for preAllocSize", size);
            }
        }
    }

    /**
     * 文件大小 【新创建时为日志文件头大小、扩大后为64M+16字节文件头】
     */
    private long currentSize;

    /**
     * Getter of preAllocSize has been added for testing
     */
    public static long getPreAllocSize() {
        return preAllocSize;
    }

    /**
     * method to allow setting preallocate size
     * of log file to pad the file.
     *
     * @param size the size to set to in bytes
     */
    public static void setPreallocSize(long size) {
        preAllocSize = size;
    }

    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }

    /**
     * 增大新日志文件到新文件大小【64M + 文件头16字节】
     * pad the current file to increase its size to the next multiple of preAllocSize greater than the current size and position
     *
     * @param fileChannel the fileChannel of the file to be padded
     * @throws IOException
     */
    long padFile(FileChannel fileChannel) throws IOException {
        long newFileSize = calculateFileSizeWithPadding(fileChannel.position(), currentSize, preAllocSize);
        if (currentSize != newFileSize) {
            fileChannel.write((ByteBuffer) fill.position(0), newFileSize - fill.remaining());
            currentSize = newFileSize;
        }
        return currentSize;
    }

    /**
     * 计算新日志文件大小 【默认16字节文件头 + 64M内容】
     * Calculates a new file size with padding. We only return a new size if
     * the current file position is sufficiently close (less than 4K) to end of
     * file and preAllocSize is &gt; 0.
     *
     * @param position     the point in the file we have written to
     * @param fileSize     application keeps track of the current file size
     * @param preAllocSize how many bytes to pad
     * @return the new file size. It can be the same as fileSize if no
     * padding was done.
     */
    // VisibleForTesting
    public static long calculateFileSizeWithPadding(long position, long fileSize, long preAllocSize) {
        // If preAllocSize is positive and we are within 4KB of the known end of the file calculate a new file size
        if (preAllocSize > 0 && position + 4096 >= fileSize) {
            // If we have written more than we have previously preallocated we need to make sure the new
            // file size is larger than what we already have
            if (position > fileSize) {
                fileSize = position + preAllocSize;
                fileSize -= fileSize % preAllocSize;
            } else {
                fileSize += preAllocSize;
            }
        }

        return fileSize;
    }

}
