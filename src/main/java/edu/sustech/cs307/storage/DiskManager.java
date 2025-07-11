package edu.sustech.cs307.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.sustech.cs307.DBEntry;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.pmw.tinylog.Logger;

/**
 * 磁盘管理器类，用于管理文件的读写和元数据的存储。
 * 提供创建文件、读取页面、写入页面、分配页面和删除文件等功能。
 * 
 * <p>
 * 该类支持从磁盘读取和写入文件，并维护文件的页面信息。
 * </p>
 */
public class DiskManager {
    private final String currentDir;
    public Map<String, Integer> filePages;

    private static final String DISK_MANAGER_META = "disk_manager_meta.json";

    public static Map<String, Integer> read_disk_manager_meta() throws DBException {
        try {
            // 确保数据库根目录存在
            if (!Files.exists(Paths.get(DBEntry.DB_NAME))) {
                Files.createDirectories(Paths.get(DBEntry.DB_NAME));
            }
            Path metaPath = Paths.get(DBEntry.DB_NAME, DISK_MANAGER_META);
            File META_FILE = metaPath.toFile();
            if (!META_FILE.exists()) {
                Logger.info("Meta file 不存在，创建新的空 meta 文件...");
                // 创建空文件并写入 {}
                try (Writer w = new FileWriter(META_FILE)) {
                    new ObjectMapper().writeValue(w, Map.of());
                }
                return new HashMap<>();
            }
            // 现有逻辑：读取 JSON
            try (Reader reader = new FileReader(META_FILE)) {
                TypeReference<Map<String, Integer>> typeRef = new TypeReference<>() {
                };
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Integer> loaded = objectMapper.readValue(reader, typeRef);
                if (loaded != null) {
                    return new HashMap<>(loaded);
                } else {
                    throw new DBException(ExceptionTypes.UnableLoadMetadata("Failed to load metadata"));
                }
            }
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.UnableLoadMetadata(e.getMessage()));
        }
    }

    /**
     * 将 DiskManager 的元数据转储到指定的元文件中。
     *
     * @param disk_manager 要转储元数据的 DiskManager 实例
     * @throws DBException 如果在写入元数据时发生错误
     */
    public static void dump_disk_manager_meta(DiskManager disk_manager) throws DBException {
        try {
            // 确保数据库根目录存在
            if (!Files.exists(Paths.get(DBEntry.DB_NAME))) {
                Files.createDirectories(Paths.get(DBEntry.DB_NAME));
            }
            Path metaPath = Paths.get(DBEntry.DB_NAME, DISK_MANAGER_META);
            File META_FILE = metaPath.toFile();
            try (Writer writer = new FileWriter(META_FILE)) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writeValue(writer, disk_manager.filePages);
            }
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.UnableSaveMetadata(e.getMessage()));
        }
    }

    public DiskManager(String path, Map<String, Integer> filePages) {
        this.currentDir = path;
        this.filePages = filePages;
    }

    public String getDbName() {
        return this.currentDir;
    }

    public String getCurrentDir() {
        return currentDir;
    }

    /**
     * 从指定文件中读取一个页面的数据到给定的 Page 对象中。
     *
     * @param page     要读取数据的 Page 对象，数据将被填充到 page.data 中。
     * @param filename 要读取的文件名。
     * @param offset   从文件中读取的起始偏移量。
     * @param length   要读取的数据长度。
     * @throws DBException 如果在读取过程中发生 I/O 错误或偏移量超出范围。
     */
    public void ReadPage(Page page, String filename, int offset, long length) throws DBException {
        // 拼接实际文件路径
        String real_path = currentDir + "/" + filename;
        try (FileInputStream fis = new FileInputStream(real_path)) {
            long seeked = fis.skip(offset);
            if (seeked < offset) {
                throw new DBException(ExceptionTypes.BadIOError(
                        String.format("Seek out of range, offset: %d, length: %d", seeked, length)));
            }
            // 直接将文件中一个页面大小的数据读取到 page.data 中
            if (fis.read(page.data.array()) != Page.DEFAULT_PAGE_SIZE) {
                // throw new DBException(ExceptionTypes.BadIOError(
                // String.format("Bad file at offset: %d, length: %d", seeked, length)));
            }
            page.position.offset = offset;
            page.position.filename = filename;
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
        }
    }

    private boolean batchMode = false;

    /**
     * 启用批量模式，减少磁盘同步次数以提高性能
     */
    public void setBatchMode(boolean enabled) {
        this.batchMode = enabled;
    }

    /**
     * 将指定页面的数据刷新到磁盘。
     *
     * @param page 要刷新的页面对象，包含数据和位置信息。
     * @throws DBException 如果在写入过程中发生输入输出错误。
     */
    public void FlushPage(Page page) throws DBException {
        // 拼接实际文件路径
        String real_path = currentDir + "/" + page.position.filename;

        // 使用 RandomAccessFile 和 FileChannel 来定位并写入数据
        try (RandomAccessFile raf = new RandomAccessFile(real_path, "rw");
                FileChannel channel = raf.getChannel()) {
            // 定位到对应的文件偏移位置
            channel.position(page.position.offset);
            // 使用 ByteBuffer.wrap 避免额外的数据拷贝
            ByteBuffer buffer = ByteBuffer.wrap(page.data.array());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            // 性能优化：批量模式下减少强制同步次数
            if (!batchMode) {
                channel.force(true);
            }
        } catch (IOException e) {
            throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
        }
    }

    /**
     * 强制同步所有待写入的数据到磁盘
     */
    public void forceSyncAll() throws DBException {
        // 这个方法可以在批量操作完成后调用，确保数据持久化
        System.gc(); // 建议GC回收，但实际同步由OS处理
    }

    /**
     * 同步磁盘管理器状态，确保元数据和文件状态的一致性
     */
    public void sync() throws DBException {
        try {
            // 1. 强制同步所有文件内容到磁盘
            forceSyncAll();

            // 2. 保存磁盘管理器元数据
            dump_disk_manager_meta(this);

            Logger.debug("DiskManager state synchronized successfully");
        } catch (Exception e) {
            Logger.error("Failed to sync DiskManager state: {}", e.getMessage());
            throw new DBException(ExceptionTypes.BadIOError("DiskManager sync failed: " + e.getMessage()));
        }
    }

    public long GetFileSize(String filename) {
        String real_path = currentDir + "/" + filename;
        File file = new File(real_path);
        return file.length();
    }

    /**
     * 检查指定的文件是否存在。
     * 
     * @param filename 要检查的文件名
     * @return 如果文件存在则返回 true，否则返回 false
     */
    public boolean fileExists(String filename) {
        String real_path = currentDir + "/" + filename;
        File file = new File(real_path);
        return file.exists();
    }

    /**
     * 创建一个新文件。
     * 
     * @param filename 文件名
     * @throws DBException 如果文件创建失败或发生IO异常
     * 
     *                     该方法会检查指定路径下是否已存在同名文件。如果不存在，则会尝试创建该文件及其上级目录。
     *                     如果创建过程中发生任何异常，将抛出DBException。
     */
    public void CreateFile(String filename) throws DBException {
        String real_path = currentDir + "/" + filename;
        File file = new File(real_path);
        if (!file.exists()) {
            try {
                // 如果上级目录不存在则创建
                File parent = file.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }
                if (!file.createNewFile()) {
                    throw new DBException(ExceptionTypes.BadIOError("File creation failed: " + real_path));
                }
                this.filePages.put(filename, 1);
            } catch (IOException e) {
                throw new DBException(ExceptionTypes.BadIOError(e.getMessage()));
            }
        }
    }

    // return file start;
    public Integer AllocatePage(String filename) throws DBException {
        Integer offset = this.filePages.get(filename);
        if (offset == null) {
            throw new DBException(ExceptionTypes.BadIOError(String.format("File not exists, %s", filename)));
        }
        this.filePages.put(filename, offset + 1);
        return offset;
    }

    public void DeleteFile(String filename) throws DBException {
        String real_path = currentDir + "/" + filename;
        File file = new File(real_path);
        if (file.exists()) {
            if (!file.delete()) {
                throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + real_path));
            }
            this.filePages.remove(filename);
        }
    }

    /**
     * 批量删除指定文件的所有页面，用于表重组操作
     * 
     * @param filename 要清空的文件名
     * @throws DBException 如果删除过程中发生错误
     */
    public void truncateFile(String filename) throws DBException {
        String real_path = currentDir + "/" + filename;
        File file = new File(real_path);

        if (file.exists()) {
            try {
                // 清空文件内容但保留文件
                try (RandomAccessFile raf = new RandomAccessFile(real_path, "rw")) {
                    raf.setLength(0);
                }
                // 重置页面计数
                this.filePages.put(filename, 1);
                Logger.debug("Truncated file: {}", filename);
            } catch (IOException e) {
                throw new DBException(ExceptionTypes.BadIOError("Failed to truncate file: " + e.getMessage()));
            }
        }
    }

    /**
     * 获取文件的页面数量
     * 
     * @param filename 文件名
     * @return 页面数量，如果文件不存在返回0
     */
    public int getPageCount(String filename) {
        Integer count = this.filePages.get(filename);
        return count != null ? count : 0;
    }
}
