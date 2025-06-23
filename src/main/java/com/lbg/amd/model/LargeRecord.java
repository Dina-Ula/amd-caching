package com.lbg.amd.model;

import lombok.Data;

import java.sql.Date;

@Data
public class LargeRecord {
    private Long id;
    private String name;
    private String email;
    private String description;
    private Date createdAt;
}
