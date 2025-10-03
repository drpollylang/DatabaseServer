package edu.uob;

public class NameValuePair {
    private String attributeName;
    private String value;

    public NameValuePair(String attributeName, String value) {
        this.attributeName = attributeName;
        this.value = value;
    }

    // Getters
    public String getAttributeName() {
        return this.attributeName;
    }
    public String getValue() {
        return this.value;
    }
}
