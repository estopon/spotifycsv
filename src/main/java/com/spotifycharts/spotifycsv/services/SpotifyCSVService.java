package com.spotifycharts.spotifycsv.services;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvException;
import com.spotifycharts.spotifycsv.model.ChartElement;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class SpotifyCSVService {
	
	@Value("${country.list}")
	private String[] countryList;
	
	@Value("${spotifycharts.url}")
	private String chartsUrl;
	
	@Value("${files.folder}")
	private String filesDirectory;
	
	public void processCSV () throws IOException, CsvException {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		ZoneId defaultZoneId = ZoneId.systemDefault();
		List<ChartElement> completeList = new ArrayList<ChartElement>();
		
		for (String country: countryList) {
			
			String urlStr = chartsUrl.replace("$1", country);
			
			LocalDate startDate = LocalDate.now().minusDays(30);
			LocalDate endDate = LocalDate.now();
			
			while (startDate.isBefore(endDate)) {
				
				String dateStr = formatter.format(Date.from(startDate.atStartOfDay(defaultZoneId).toInstant()));
				
				urlStr = urlStr.replace("$2", dateStr);
				
				URL url = new URL(urlStr);
				URLConnection conn = url.openConnection();
				conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36");
				
				InputStream in = conn.getInputStream();
				
				String fileName = filesDirectory + country + "_" + dateStr + ".csv";
				
				Files.copy(in, Paths.get(fileName));	
				
				FileReader fileReader = new FileReader(fileName);
				CsvToBeanBuilder csvReader = new CsvToBeanBuilder(fileReader);
				
				List<ChartElement> charts = csvReader
						.withSkipLines(2)
		                .withType(ChartElement.class)
		                .build()
		                .parse();
				
				for (ChartElement chart: charts) {
					chart.setCountry(country);
					chart.setDate(dateStr);
				}
				
				completeList.addAll(charts);
				
				fileReader.close();
				
				Path filePath = Paths.get(fileName);
				Files.delete(filePath);
				
				startDate = startDate.plusDays(1);
			}
			
			
		}
		
	}

}
