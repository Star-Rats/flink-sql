package com.jmy.flinksql.app;

public class UserSchool {
    private Integer userId;
    private String school;

    public UserSchool() {
    }

    public UserSchool(Integer userId, String school) {
        this.userId = userId;
        this.school = school;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }
}
