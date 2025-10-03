package edu.uob;

// Instantiated by QueryParser, and returned to QueryHandler
import edu.uob.Token.Keyword;
import edu.uob.Token.TargetType;
import edu.uob.Token.AlterationType;

import java.util.List;
import java.util.ArrayList;

public class ParsedQuery {
    private Keyword commandType;
    private TargetType targetType;
    private String databaseName;
    private String tableName1;
    private String tableName2;
    private List<String> attributeList;
    private AlterationType alterationType;
    private ArrayList<String> valueList;
    private Condition condition;
    private ArrayList<NameValuePair> nameValueList;
    private String joinAttribute1;
    private String joinAttribute2;


    public ParsedQuery() {
        // When parsedQuery is instantiated, all fields are set to null.
        // Setting fields is done by QueryParser, according to what information
        // is contained within a particular query. The QueryHandler then checks which
        // fields are populated and which are empty in order to make decisions
        this.commandType = null;
        this.targetType = null;
        this.databaseName = null;
        this.tableName1 = null;
        this.tableName2 = null;
        this.attributeList = null;
        this.alterationType = null;
        this.valueList = null;
        this.condition = null;
        this.nameValueList = null;
        this.joinAttribute1 = null;
        this.joinAttribute2 = null;
    }

    // Getters and setters for all attributes. Set by QueryParser and got by QueryHandler
    public Keyword getCommandType() {
        return this.commandType;
    }
    public void setCommandType(Keyword commandType) {
        this.commandType = commandType;
    }

    public TargetType getTargetType() {
        return this.targetType;
    }
    public void setTargetType(TargetType targetType) {
        this.targetType = targetType;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName1() {
        return this.tableName1;
    }
    public void setTableName1(String tableName) {
        this.tableName1 = tableName;
    }

    public String getTableName2() {
        return this.tableName2;
    }
    public void setTableName2(String tableName) {
        this.tableName2 = tableName;
    }

    public List<String> getAttributeList() {
        return this.attributeList;
    }
    public void setAttributeList(List<String> attributeList) {
        this.attributeList = attributeList;
    }

    public AlterationType getAlterationType() {
        return this.alterationType;
    }
    public void setAlterationType(AlterationType alterationType) {
        this.alterationType = alterationType;
    }

    public ArrayList<String> getValueList() {
        return this.valueList;
    }
    public void setValueList(ArrayList<String> valueList) {
        this.valueList = valueList;
    }

    public Condition getCondition() {
        return this.condition;
    }
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public ArrayList<NameValuePair> getNameValueList() {
        return this.nameValueList;
    }
    public void setNameValueList(ArrayList<NameValuePair> nameValueList) {
        this.nameValueList = nameValueList;
    }

    public String getJoinAttribute1() {
        return this.joinAttribute1;
    }
    public void setJoinAttribute1(String joinAttribute) {
        this.joinAttribute1 = joinAttribute;
    }

    public String getJoinAttribute2() {
        return this.joinAttribute2;
    }
    public void setJoinAttribute2(String joinAttribute) {
        this.joinAttribute2 = joinAttribute;
    }
}
