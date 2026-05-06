package edu.sustech.cs307.storage.replacer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClockReplacer implements PageReplacer{
    private List<Integer> frames;
    private Map<Integer, Boolean> pinnedFrames;
    private Map<Integer, Boolean> referenceBits;
    private int maxSize;
    private int clockHand;

    public ClockReplacer(int numPages) {
        this.maxSize = numPages;
        this.frames = new ArrayList<>();
        this.pinnedFrames = new HashMap<>();
        this.referenceBits = new HashMap<>();
        this.clockHand = 0;
    }

    @Override
    public int Victim() {
        if (frames.isEmpty()) {
            return -1;
        }
        int scanned = 0;
        while (scanned < frames.size() * 2) {
            if (clockHand >= frames.size()) {
                clockHand = 0;
            }
            int frameId = frames.get(clockHand);
            if (!pinnedFrames.getOrDefault(frameId, false)) {
                if (referenceBits.getOrDefault(frameId, false)) {
                    referenceBits.put(frameId, false);
                } else {
                    frames.remove(clockHand);
                    pinnedFrames.remove(frameId);
                    referenceBits.remove(frameId);
                    if (clockHand >= frames.size() && !frames.isEmpty()) {
                        clockHand = 0;
                    }
                    return frameId;
                }
            }
            clockHand++;
            scanned++;
        }
        return -1;
    }

    @Override
    public void Pin(int frameId) {
        if (pinnedFrames.containsKey(frameId)) {
            pinnedFrames.put(frameId, true);
            return;
        }
        if (frames.size() >= maxSize) {
            throw new RuntimeException("REPLACER IS FULL");
        }
        frames.add(frameId);
        pinnedFrames.put(frameId, true);
        referenceBits.put(frameId, true);
    }

    @Override
    public void Unpin(int frameId) {
        if (!pinnedFrames.containsKey(frameId) || !pinnedFrames.get(frameId)) {
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }
        pinnedFrames.put(frameId, false);
        referenceBits.put(frameId, true);
    }

    @Override
    public int size() {
        return frames.size();
    }

    @Override
    public void Reset() {
        frames.clear();
        pinnedFrames.clear();
        referenceBits.clear();
        clockHand = 0;
    }
}
