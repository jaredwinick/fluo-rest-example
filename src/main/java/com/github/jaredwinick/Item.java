package com.github.jaredwinick;

public class Item {
    public Item() {}
    public Item(String id, Integer value) {
        this.id = id;
        this.value = value;
    }
    private String id;
    private Integer value;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id='" + id + '\'' +
                ", value=" + value +
                '}';
    }
}
