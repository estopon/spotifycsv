package com.spotifycharts.spotifycsv.model;

import java.util.ArrayList;

import com.opencsv.bean.CsvBindByPosition;

import lombok.Data;

@Data
public class ChartElement {
	
	@CsvBindByPosition(position = 0)
	private int Position;
	
	@CsvBindByPosition(position = 1)
	private String trackName;
	
	@CsvBindByPosition(position = 2)
	private String artist;
	
	@CsvBindByPosition(position = 3)
	private int streams;
	
	@CsvBindByPosition(position = 4)
	private String url;	
	
	private String country;
	
	private String date;
	
	private ArrayList<String> genres;

}
