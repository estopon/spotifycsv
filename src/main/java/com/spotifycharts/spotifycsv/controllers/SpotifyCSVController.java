package com.spotifycharts.spotifycsv.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.spotifycharts.spotifycsv.services.SpotifyCSVService;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class SpotifyCSVController {
	
	@Autowired
	private SpotifyCSVService spotifyCSVService;
	
	@RequestMapping("/")
    public String index() {
		
		try {
			spotifyCSVService.processCSV();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		
		return "OK";
	}

}
