package com.crimson.jobq.ratelimit;


import jakarta.servlet.http.HttpServletRequest;

public final class ClientIds {
    private ClientIds(){}

    public static String resolve(HttpServletRequest req){
        String id = req.getHeader("X-Client-Id");
        if(id == null || id.isBlank()) id = req.getRemoteAddr();

        return id.length() > 64 ? id.substring(0, 64) : id;
    }
}
