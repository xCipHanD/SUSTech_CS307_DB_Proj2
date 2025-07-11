package edu.sustech.cs307.record;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.storage.Page;
import edu.sustech.cs307.storage.PagePosition;
import io.netty.buffer.ByteBuf;

/**
 * 记录文件处理类，负责管理记录文件的读取、插入、删除和更新操作。
 *
 * <p>
 * 该类使用 DiskManager 和 BufferPool 来处理文件的页和缓冲区，提供对记录的基本操作。
 * </p>
 *
 * <p>
 * 主要功能包括：
 * </p>
 * <ul>
 * <li>读取文件头信息以获取页面数量</li>
 * <li>插入新记录并管理空闲页面</li>
 * <li>删除和更新现有记录</li>
 * <li>根据记录 ID 获取记录</li>
 * </ul>
 *
 * <p>
 * 异常处理：在操作过程中，如果遇到文件损坏或页面超出范围等情况，将抛出 DBException 或 RuntimeException。
 * </p>
 */
public class RecordFileHandle {
    DiskManager diskManager;
    BufferPool bufferPool;
    String filename;
    RecordFileHeader fileHeader;

    public RecordFileHandle(DiskManager diskManager, BufferPool bufferPool, String filename, RecordFileHeader header)
            throws DBException {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
        this.filename = filename;
        this.fileHeader = header;
    }

    /**
     * 获取记录文件的头部信息。
     *
     * @return 返回当前记录文件的头部对象 {@link RecordFileHeader}。
     */
    public RecordFileHeader getFileHeader() {
        return fileHeader;
    }

    /**
     * 获取记录文件的文件名。
     *
     * @return 文件名字符串
     */
    public String getFilename() {
        return filename;
    }

    /**
     * 检查指定的记录 ID 是否存在于记录文件中。
     *
     * @param rid 记录 ID，包含页面编号和槽编号。
     * @return 如果记录存在，则返回 true；否则返回 false。
     * @throws DBException 如果在获取页面句柄时发生数据库异常。
     */
    public boolean IsRecord(RID rid) throws DBException {
        RecordPageHandle page_handle = FetchPageHandle(rid.pageNum);
        return BitMap.isSet(page_handle.bitmap, rid.slotNum);
    }

    /**
     * 根据给定的记录标识符 (RID) 获取相应的记录。
     *
     * @param rid 记录标识符，包含页面编号和槽编号。
     * @return 返回对应的记录对象。
     * @throws DBException 如果在获取记录过程中发生数据库异常。
     */
    public Record GetRecord(RID rid) throws DBException {
        RecordPageHandle handle = FetchPageHandle(rid.pageNum);
        Record record = new Record(handle.getSlot(rid.slotNum));
        bufferPool.unpin_page(handle.page.position, false);
        return record;
    }

    /**
     * 向记录文件中插入一条记录。
     *
     * @param buf 包含要插入记录的字节缓冲区
     * @return 插入记录的 RID（记录标识符）
     * @throws DBException      数据库异常
     * @throws RuntimeException 如果无法分配空间
     */
    public RID InsertRecord(ByteBuf buf) throws DBException {
        RecordPageHandle pageHandle = null;
        int slotNum = -1;

        // 循环查找有空闲槽位的页面
        while (true) {
            pageHandle = create_page_handle();
            slotNum = BitMap.firstBit(false, pageHandle.bitmap, fileHeader.getNumberOfRecordsPrePage());

            // 如果找到空闲槽位，跳出循环
            if (slotNum < fileHeader.getNumberOfRecordsPrePage()) {
                break;
            }

            // 当前页面已满，需要从空闲页面链表中移除
            if (fileHeader.getFirstFreePage() == pageHandle.page.getPageID()) {
                fileHeader.setFirstFreePage(pageHandle.pageHdr.getNextFreePageNo());
            }

            // 解除当前页面的固定状态
            bufferPool.unpin_page(pageHandle.page.position, false);

            // 如果没有更多空闲页面，创建新页面并跳出循环
            if (fileHeader.getFirstFreePage() == RecordPageHeader.NO_NEXT_FREE_PAGE) {
                pageHandle = CreateNewPageHandle();
                slotNum = BitMap.firstBit(false, pageHandle.bitmap, fileHeader.getNumberOfRecordsPrePage());
                break;
            }
        }

        // 插入记录
        ByteBuf slot = pageHandle.getSlot(slotNum).clear();
        slot.writeBytes(buf, 0, fileHeader.getRecordSize());
        BitMap.set(pageHandle.bitmap, slotNum);
        pageHandle.pageHdr.setNumberOfRecords(pageHandle.pageHdr.getNumberOfRecords() + 1);

        // 如果页面变满，从空闲页面链表中移除
        if (pageHandle.pageHdr.getNumberOfRecords() == fileHeader.getNumberOfRecordsPrePage()) {
            if (fileHeader.getFirstFreePage() == pageHandle.page.getPageID()) {
                fileHeader.setFirstFreePage(pageHandle.pageHdr.getNextFreePageNo());
            }
        }

        bufferPool.unpin_page(pageHandle.page.position, true);

        return new RID(pageHandle.page.getPageID(), slotNum);
    }

    /**
     * 删除指定 RID 的记录。
     *
     * @param rid 要删除的记录标识符
     * @throws DBException 如果在删除过程中发生数据库异常
     */
    public void DeleteRecord(RID rid) throws DBException {
        RecordPageHandle pageHandle = FetchPageHandle(rid.pageNum);

        // 检查页面在删除前是否已满
        boolean wasPageFull = (pageHandle.pageHdr.getNumberOfRecords() == fileHeader.getNumberOfRecordsPrePage());

        BitMap.reset(pageHandle.bitmap, rid.slotNum);
        pageHandle.pageHdr.setNumberOfRecords(pageHandle.pageHdr.getNumberOfRecords() - 1);

        // 如果页面之前是满的，现在有了空闲槽位，将其加入空闲页面链表
        if (wasPageFull && pageHandle.pageHdr.getNumberOfRecords() < fileHeader.getNumberOfRecordsPrePage()) {
            pageHandle.pageHdr.setNextFreePageNo(fileHeader.getFirstFreePage());
            fileHeader.setFirstFreePage(pageHandle.page.getPageID());
        }

        bufferPool.unpin_page(pageHandle.page.position, true);
    }

    /**
     * 更新指定记录的内容。
     *
     * @param rid 记录标识符，包含页面编号和槽编号。
     * @param buf 包含新记录数据的字节缓冲区。
     * @throws DBException 如果在更新过程中发生数据库异常。
     */
    public void UpdateRecord(RID rid, ByteBuf buf) throws DBException {
        RecordPageHandle pageHandle = FetchPageHandle(rid.pageNum);
        ByteBuf slot = pageHandle.getSlot(rid.slotNum);
        slot.clear();
        // Write only up to the record size to avoid overflow
        slot.writeBytes(buf, 0, fileHeader.getRecordSize());
        bufferPool.unpin_page(pageHandle.page.position, true);
    }

    /**
     * 从记录文件中获取指定页面的页面句柄。
     *
     * @param pageId 要获取的页面的 ID。
     * @return 返回对应页面的 RecordPageHandle。
     * @throws DBException 如果页面 ID 超出范围或页面无法从缓冲池中获取。
     */
    public RecordPageHandle FetchPageHandle(int pageId) throws DBException {
        if (pageId > fileHeader.getNumberOfPages()) {
            throw new RuntimeException(String.format("%s: pageId %d is out of range", filename, pageId));
        }
        PagePosition pagePosition = new PagePosition(filename, pageId * Page.DEFAULT_PAGE_SIZE);
        Page page = bufferPool.FetchPage(pagePosition);
        if (page == null) {
            throw new RuntimeException(String.format("%s: pageId %d is out of range", filename, pageId));
        }
        return new RecordPageHandle(fileHeader, page);
    }

    public void UnpinPageHandle(int pageId, boolean is_dirty) throws DBException {
        bufferPool.unpin_page(new PagePosition(filename, pageId), is_dirty);
    }

    /**
     * 创建一个新的记录页面句柄。
     * 此方法从缓冲池中分配一个新页面，并初始化该页面的位图和头信息。
     * 更新文件头以反映新页面的添加，并设置新页面的编号和下一个空闲页面编号。
     *
     * @return 新创建的记录页面句柄
     * @throws DBException 如果在创建新页面时发生数据库异常
     */
    public RecordPageHandle CreateNewPageHandle() throws DBException {
        Page newPage = bufferPool.NewPage(filename);
        if (newPage == null) {
            throw new DBException(ExceptionTypes.BadIOError("Unable to allocate new page"));
        }

        // 确保页面数据完全清零
        newPage.data.setZero(0, newPage.data.capacity());

        RecordPageHandle pageHandle = new RecordPageHandle(fileHeader, newPage);

        // Initialize the page - 完全初始化页面
        BitMap.init(pageHandle.bitmap);
        pageHandle.pageHdr.setNumberOfRecords(0);
        pageHandle.pageHdr.setNextFreePageNo(RecordPageHeader.NO_NEXT_FREE_PAGE);

        // Update the file header - increment page count
        fileHeader.setNumberOfPages(fileHeader.getNumberOfPages() + 1);

        // 将新页面设置为首个空闲页面，因为它有空闲槽位
        pageHandle.pageHdr.setNextFreePageNo(fileHeader.getFirstFreePage());
        fileHeader.setFirstFreePage(pageHandle.page.getPageID());

        // 立即刷新页面确保初始化数据写入磁盘
        bufferPool.FlushPage(pageHandle.page.position);

        return pageHandle;
    }

    /**
     * 创建一个记录页面句柄。如果没有可用的空闲页面，则创建一个新页面句柄；
     * 否则，返回第一个空闲页面的句柄。
     *
     * @return 返回一个记录页面句柄
     * @throws DBException 如果在创建或获取页面句柄时发生数据库异常
     */
    private RecordPageHandle create_page_handle() throws DBException {
        // 清理无效的空闲页面
        while (fileHeader.getFirstFreePage() != RecordPageHeader.NO_NEXT_FREE_PAGE) {
            int freePageId = fileHeader.getFirstFreePage();

            // 检查页面ID是否有效
            if (freePageId <= 0 || freePageId > fileHeader.getNumberOfPages()) {
                // 无效页面ID，跳过
                fileHeader.setFirstFreePage(RecordPageHeader.NO_NEXT_FREE_PAGE);
                break;
            }

            try {
                RecordPageHandle pageHandle = FetchPageHandle(freePageId);
                // 检查页面是否真的有空闲槽位
                int slotNum = BitMap.firstBit(false, pageHandle.bitmap, fileHeader.getNumberOfRecordsPrePage());
                if (slotNum < fileHeader.getNumberOfRecordsPrePage()) {
                    // 找到有效的空闲页面
                    return pageHandle;
                } else {
                    // 页面已满，从空闲链表中移除
                    fileHeader.setFirstFreePage(pageHandle.pageHdr.getNextFreePageNo());
                    bufferPool.unpin_page(pageHandle.page.position, false);
                }
            } catch (RuntimeException e) {
                // 页面无效，从空闲链表中移除
                fileHeader.setFirstFreePage(RecordPageHeader.NO_NEXT_FREE_PAGE);
                break;
            }
        }

        // 没有有效的空闲页面，创建新页面
        return CreateNewPageHandle();
    }
}
