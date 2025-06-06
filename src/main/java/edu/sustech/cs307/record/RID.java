package edu.sustech.cs307.record;

public class RID {
    public int pageNum;
    public int slotNum;

    public RID(int page_no, int slot_no) {
        this.pageNum = page_no;
        this.slotNum = slot_no;
    }

    public RID(RID rid) {
        this.pageNum = rid.pageNum;
        this.slotNum = rid.slotNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        RID rid = (RID) obj;
        return pageNum == rid.pageNum && slotNum == rid.slotNum;
    }

    @Override
    public int hashCode() {
        return 31 * pageNum + slotNum;
    }

    @Override
    public String toString() {
        return "RID(" + pageNum + "," + slotNum + ")";
    }
}
