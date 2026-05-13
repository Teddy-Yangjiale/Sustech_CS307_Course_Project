package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class InMemoryOrderedIndex implements Index {
    private static final int DEFAULT_ORDER = 4;

    private final int order;
    private Node root;
    private LeafNode firstLeaf;

    private abstract static class Node {
        final ArrayList<Value> keys = new ArrayList<>();
        InternalNode parent;

        abstract boolean isLeaf();
    }

    private static class InternalNode extends Node {
        final ArrayList<Node> children = new ArrayList<>();

        @Override
        boolean isLeaf() {
            return false;
        }
    }

    private static class LeafNode extends Node {
        final ArrayList<ArrayList<RID>> values = new ArrayList<>();
        LeafNode next;

        @Override
        boolean isLeaf() {
            return true;
        }
    }

    public InMemoryOrderedIndex() {
        this(DEFAULT_ORDER);
    }

    public InMemoryOrderedIndex(int order) {
        this.order = Math.max(3, order);
        LeafNode leaf = new LeafNode();
        this.root = leaf;
        this.firstLeaf = leaf;
    }

    @Override
    public List<RID> equalTo(Value value) throws DBException {
        LeafNode leaf = findLeaf(value);
        int index = keyIndex(leaf.keys, value);
        if (index < leaf.keys.size() && compare(leaf.keys.get(index), value) == 0) {
            return copyRids(leaf.values.get(index));
        }
        return List.of();
    }

    @Override
    public List<RID> lessThan(Value value, boolean isEqual) throws DBException {
        ArrayList<RID> result = new ArrayList<>();
        for (LeafNode leaf = firstLeaf; leaf != null; leaf = leaf.next) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                int cmp = compare(leaf.keys.get(i), value);
                if (cmp < 0 || (isEqual && cmp == 0)) {
                    result.addAll(copyRids(leaf.values.get(i)));
                } else {
                    return result;
                }
            }
        }
        return result;
    }

    @Override
    public List<RID> moreThan(Value value, boolean isEqual) throws DBException {
        ArrayList<RID> result = new ArrayList<>();
        LeafNode leaf = findLeaf(value);
        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                int cmp = compare(leaf.keys.get(i), value);
                if (cmp > 0 || (isEqual && cmp == 0)) {
                    result.addAll(copyRids(leaf.values.get(i)));
                }
            }
            leaf = leaf.next;
        }
        return result;
    }

    @Override
    public List<RID> range(Value low, Value high, boolean leftEqual, boolean rightEqual) throws DBException {
        ArrayList<RID> result = new ArrayList<>();
        LeafNode leaf = findLeaf(low);
        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                Value key = leaf.keys.get(i);
                int lowCmp = compare(key, low);
                int highCmp = compare(key, high);
                boolean geLow = lowCmp > 0 || (leftEqual && lowCmp == 0);
                boolean leHigh = highCmp < 0 || (rightEqual && highCmp == 0);
                if (geLow && leHigh) {
                    result.addAll(copyRids(leaf.values.get(i)));
                }
                if (highCmp > 0 || (!rightEqual && highCmp == 0)) {
                    return result;
                }
            }
            leaf = leaf.next;
        }
        return result;
    }

    @Override
    public void insert(Value value, RID rid) throws DBException {
        LeafNode leaf = findLeaf(value);
        int index = keyIndex(leaf.keys, value);
        if (index < leaf.keys.size() && compare(leaf.keys.get(index), value) == 0) {
            if (!leaf.values.get(index).contains(rid)) {
                leaf.values.get(index).add(new RID(rid));
            }
            return;
        }
        leaf.keys.add(index, value);
        ArrayList<RID> ridList = new ArrayList<>();
        ridList.add(new RID(rid));
        leaf.values.add(index, ridList);
        if (leaf.keys.size() > order - 1) {
            splitLeaf(leaf);
        }
    }

    @Override
    public void delete(Value value, RID rid) throws DBException {
        LeafNode leaf = findLeaf(value);
        int index = keyIndex(leaf.keys, value);
        if (index >= leaf.keys.size() || compare(leaf.keys.get(index), value) != 0) {
            return;
        }
        leaf.values.get(index).remove(rid);
        if (leaf.values.get(index).isEmpty()) {
            leaf.keys.remove(index);
            leaf.values.remove(index);
            refreshParentKeys(leaf);
        }
    }

    @Override
    public String printTree() {
        StringBuilder result = new StringBuilder();
        Queue<Node> queue = new ArrayDeque<>();
        queue.add(root);
        int level = 0;
        while (!queue.isEmpty()) {
            int size = queue.size();
            result.append("Level ").append(level).append(": ");
            for (int i = 0; i < size; i++) {
                Node node = queue.remove();
                result.append(node.isLeaf() ? "Leaf" : "Internal");
                result.append(node.keys).append(" ");
                if (node instanceof InternalNode internalNode) {
                    queue.addAll(internalNode.children);
                }
            }
            result.append(System.lineSeparator());
            level++;
        }
        return result.toString();
    }

    private LeafNode findLeaf(Value value) throws DBException {
        Node node = root;
        while (!node.isLeaf()) {
            InternalNode internal = (InternalNode) node;
            int childIndex = childIndex(internal.keys, value);
            node = internal.children.get(childIndex);
        }
        return (LeafNode) node;
    }

    private void splitLeaf(LeafNode leaf) throws DBException {
        int split = (leaf.keys.size() + 1) / 2;
        LeafNode right = new LeafNode();
        right.parent = leaf.parent;
        right.keys.addAll(leaf.keys.subList(split, leaf.keys.size()));
        right.values.addAll(leaf.values.subList(split, leaf.values.size()));
        leaf.keys.subList(split, leaf.keys.size()).clear();
        leaf.values.subList(split, leaf.values.size()).clear();
        right.next = leaf.next;
        leaf.next = right;
        insertIntoParent(leaf, right.keys.get(0), right);
    }

    private void insertIntoParent(Node left, Value separator, Node right) throws DBException {
        if (left.parent == null) {
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(separator);
            newRoot.children.add(left);
            newRoot.children.add(right);
            left.parent = newRoot;
            right.parent = newRoot;
            root = newRoot;
            return;
        }
        InternalNode parent = left.parent;
        int leftIndex = parent.children.indexOf(left);
        parent.keys.add(leftIndex, separator);
        parent.children.add(leftIndex + 1, right);
        right.parent = parent;
        if (parent.keys.size() > order - 1) {
            splitInternal(parent);
        }
    }

    private void splitInternal(InternalNode node) throws DBException {
        int mid = node.keys.size() / 2;
        Value separator = node.keys.get(mid);
        InternalNode right = new InternalNode();
        right.parent = node.parent;
        right.keys.addAll(node.keys.subList(mid + 1, node.keys.size()));
        right.children.addAll(node.children.subList(mid + 1, node.children.size()));
        for (Node child : right.children) {
            child.parent = right;
        }
        node.keys.subList(mid, node.keys.size()).clear();
        node.children.subList(mid + 1, node.children.size()).clear();
        insertIntoParent(node, separator, right);
    }

    private void refreshParentKeys(Node node) {
        while (node.parent != null) {
            InternalNode parent = node.parent;
            int index = parent.children.indexOf(node);
            if (index > 0 && !node.keys.isEmpty()) {
                parent.keys.set(index - 1, firstKey(node));
            }
            node = parent;
        }
    }

    private Value firstKey(Node node) {
        while (!node.isLeaf()) {
            node = ((InternalNode) node).children.get(0);
        }
        return node.keys.isEmpty() ? null : node.keys.get(0);
    }

    private int keyIndex(List<Value> keys, Value value) throws DBException {
        int low = 0;
        int high = keys.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compare(keys.get(mid), value) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int childIndex(List<Value> keys, Value value) throws DBException {
        int low = 0;
        int high = keys.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (compare(keys.get(mid), value) <= 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private int compare(Value left, Value right) throws DBException {
        return ValueComparer.compare(left, right);
    }

    private ArrayList<RID> copyRids(List<RID> source) {
        ArrayList<RID> result = new ArrayList<>();
        for (RID rid : source) {
            result.add(new RID(rid));
        }
        return result;
    }
}
