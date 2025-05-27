package edu.sustech.cs307.index;

import java.util.ArrayList;
import java.util.List;

public class BTree<K extends Comparable<K>, V> {
    private static class Node<K, V> {
        boolean isLeaf;
        List<K> keys = new ArrayList<>();
        List<V> values = new ArrayList<>();
        List<Node<K, V>> children = new ArrayList<>();

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }
    }

    private final int t;
    private Node<K, V> root;

    public BTree(int t) {
        this.t = t;
        this.root = new Node<>(true);
    }

    public V search(K key) {
        return search(root, key);
    }

    private V search(Node<K, V> node, K key) {
        int i = 0;
        while (i < node.keys.size() && key.compareTo(node.keys.get(i)) > 0) i++;
        if (i < node.keys.size() && key.compareTo(node.keys.get(i)) == 0) {
            if (node.isLeaf) return node.values.get(i);
        }
        if (node.isLeaf) return null;
        return search(node.children.get(i), key);
    }

    public void insert(K key, V value) {
        Node<K, V> r = root;
        if (r.keys.size() == 2 * t - 1) {
            Node<K, V> s = new Node<>(false);
            s.children.add(r);
            splitChild(s, 0, r);
            root = s;
            insertNonFull(s, key, value);
        } else {
            insertNonFull(r, key, value);
        }
    }

    private void insertNonFull(Node<K, V> node, K key, V value) {
        int i = node.keys.size() - 1;
        if (node.isLeaf) {
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) i--;
            node.keys.add(i + 1, key);
            node.values.add(i + 1, value);
        } else {
            while (i >= 0 && key.compareTo(node.keys.get(i)) < 0) i--;
            i++;
            if (node.children.get(i).keys.size() == 2 * t - 1) {
                splitChild(node, i, node.children.get(i));
                if (key.compareTo(node.keys.get(i)) > 0) i++;
            }
            insertNonFull(node.children.get(i), key, value);
        }
    }

    private void splitChild(Node<K, V> parent, int i, Node<K, V> y) {
        Node<K, V> z = new Node<>(y.isLeaf);
        for (int j = 0; j < t - 1; j++) {
            z.keys.add(y.keys.remove(t));
            if (y.isLeaf) z.values.add(y.values.remove(t));
        }
        if (!y.isLeaf) {
            for (int j = 0; j < t; j++) {
                z.children.add(y.children.remove(t));
            }
        }
        parent.keys.add(i, y.keys.remove(t - 1));
        parent.children.add(i + 1, z);
    }

    // 你可以继续扩展遍历等方法
}
