package com.example;

import lombok.Data;

import java.util.List;

@Data
public class RedisApiResponse {
    private List<String> result;
}
