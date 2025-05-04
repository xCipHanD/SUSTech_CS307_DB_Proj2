package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    private final Set<Integer> pinnedFrames = new HashSet<>();
    private final Set<Integer> LRUHash = new HashSet<>();
    private final LinkedList<Integer> LRUList = new LinkedList<>();

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    public int Victim() {
        if (LRUList.isEmpty()) {
            return -1;
        }
        int frameId = LRUList.removeFirst();
        LRUHash.remove(frameId);
        return frameId;
    }

    public void Pin(int frameId) {
        // 已存在
        if (pinnedFrames.contains(frameId)) {
            return;
        }
        if (pinnedFrames.size() == maxSize || LRUHash.size() == maxSize) {
            throw new RuntimeException("REPLACER IS FULL");
        }

        if (LRUHash.contains(frameId)) {
            LRUList.removeFirstOccurrence(frameId);
            LRUHash.remove(frameId);
        }
        pinnedFrames.add(frameId);
    }


    public void Unpin(int frameId) {
        if (LRUHash.contains(frameId)) {
            return;
        }
        if (pinnedFrames.contains(frameId)) {
            pinnedFrames.remove(frameId);
            LRUHash.add(frameId);
            LRUList.addLast(frameId);
        }else{
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }
    }


    public int size() {
        return LRUList.size() + pinnedFrames.size();
    }
}