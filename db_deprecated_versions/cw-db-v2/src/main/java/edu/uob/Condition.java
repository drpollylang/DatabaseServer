package edu.uob;

import edu.uob.Token.*;

public class Condition {
    private Condition condition1;
    private BooleanOperator booleanOperator;
    private Condition condition2;
    private String attributeName;
    private String comparator;
    private String value;


    public Condition() {
        this.condition1 = null;
        this.booleanOperator = null;
        this.condition2 = null;
        this.attributeName = null;
        this.comparator = null;
        this.value = null;
    }

    public enum ConditionType {
        NON_TERMINAL,
        TERMINAL
    }

    public ConditionType getTypeOfCondition() throws DBException {
        if (!(this.condition1 == null || this.booleanOperator == null ||
                this.condition2 == null)) {
            return ConditionType.NON_TERMINAL;
        }
        else if (!(this.attributeName == null || this.comparator == null ||
                this.value == null)) {
            return ConditionType.TERMINAL;
        }
        else {
            throw new DBException("Malformed condition.");
        }
    }


    // Getters and setters
    public Condition getCondition1() {
        return this.condition1;
    }
    public void setCondition1(Condition condition1) {
        this.condition1 = condition1;
    }
    public BooleanOperator getBooleanOperator() {
        return this.booleanOperator;
    }
    public void setBooleanOperator(BooleanOperator booleanOperator) {
        this.booleanOperator = booleanOperator;
    }
    public Condition getCondition2() {
        return this.condition2;
    }
    public void setCondition2(Condition condition2) {
        this.condition2 = condition2;
    }
    public String getAttributeName() {
        return this.attributeName;
    }
    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }
    public String getComparator() {
        return this.comparator;
    }
    public void setComparator(String comparator) {
        this.comparator = comparator;
    }
    public String getValue() {
        return this.value;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
