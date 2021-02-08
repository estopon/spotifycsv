package com.spotifycharts.spotifycsv.services;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvException;
import com.spotifycharts.spotifycsv.model.ChartElement;
import com.spotifycharts.spotifycsv.model.Genre;
import com.wrapper.spotify.SpotifyApi;
import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.credentials.ClientCredentials;
import com.wrapper.spotify.model_objects.specification.Artist;
import com.wrapper.spotify.model_objects.specification.ArtistSimplified;
import com.wrapper.spotify.model_objects.specification.Track;
import com.wrapper.spotify.requests.authorization.client_credentials.ClientCredentialsRequest;
import com.wrapper.spotify.requests.data.artists.GetArtistRequest;
import com.wrapper.spotify.requests.data.tracks.GetTrackRequest;

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
	
	@Value("${clientID}")
	private String clientID;
	
	@Value("${clientSecret}")
	private String clientSecret;
	
	@Value("${numDays}")
	private String numDays;
	
	@Value("${output.filename}")
	private String outputFilename;
	
	public void processCSV () throws IOException, CsvException, ParseException, SpotifyWebApiException, InterruptedException {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		ZoneId defaultZoneId = ZoneId.systemDefault();
		List<ChartElement> completeList = new ArrayList<ChartElement>();
		
		SpotifyApi spotifyApi = new SpotifyApi.Builder()
			    .setClientId(clientID)
			    .setClientSecret(clientSecret)
			    .build();
		
		ClientCredentialsRequest clientCredentialsRequest = spotifyApi.clientCredentials()
			    .build();
		
		ClientCredentials clientCredentials = clientCredentialsRequest.execute();
		
		spotifyApi.setAccessToken(clientCredentials.getAccessToken());
		
		log.debug("Token Expires in: " + clientCredentials.getExpiresIn());
		
		for (String country: countryList) {
			
			String urlStr = chartsUrl.replace("$1", country);
			
			LocalDate startDate = LocalDate.now().minusDays(Long.parseLong(numDays)+1);
			LocalDate endDate = LocalDate.now().minusDays(1);
			
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
					
					System.out.println(country + " - " + dateStr + " - " + chart.getPosition() + "/" + charts.size());
					
					String trackID = chart.getUrl();
					int index=trackID.lastIndexOf('/');
					trackID = trackID.substring(index+1);
					
					// GetAudioFeaturesForTrackRequest getAudioFeaturesForTrackRequest = spotifyApi.getAudioFeaturesForTrack(trackID).build();
					// AudioFeatures audioFeatures = getAudioFeaturesForTrackRequest.execute();
					
					GetTrackRequest getTrackRequest = spotifyApi.getTrack(trackID).build();
					Track track = getTrackRequest.execute();
					
					for (ArtistSimplified artistS: track.getArtists()) {
						GetArtistRequest getArtistRequest = spotifyApi.getArtist(artistS.getId()).build();
						Artist artist = getArtistRequest.execute();
						
						ArrayList<String> genres = new ArrayList<String>(); 
						for (String genre: artist.getGenres()) {
							genres.add(genre);
						}
						chart.setGenres(genres);
					}
					
					chart.setCountry(country);
					chart.setDate(dateStr);
					
					Thread.sleep(500);
				}
				
				completeList.addAll(charts);
				
				fileReader.close();
				
				Path filePath = Paths.get(fileName);
				Files.delete(filePath);
				
				startDate = startDate.plusDays(1);
			}			
			
		}
		
		if (!completeList.isEmpty()) {
			List<Genre> genreList = new ArrayList<Genre>();
			for (ChartElement chart: completeList) {
				for (String genreStr: chart.getGenres()) {
					Genre genre = new Genre();
					genre.setGenre(genreStr);
					genre.setCountry(chart.getCountry());
					genre.setStreams(chart.getStreams());
					genreList.add(genre);
				}				
			}
			
			Map<String, Map<String, Integer>> genreMap = genreList.stream().collect(
				Collectors.groupingBy(
						Genre::getGenre,
						Collectors.groupingBy(
								Genre::getCountry,
								Collectors.mapping(
										Genre::getStreams,
										Collectors.reducing(0, a -> a, (a1, a2) -> a1 + a2)
								)
						)				
				)	
			);
			
			Writer writer = Files.newBufferedWriter(Paths.get(filesDirectory+outputFilename));
			StatefulBeanToCsv<Genre> beanToCsv = new StatefulBeanToCsvBuilder<Genre>(writer)
                    .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                    .build();
			
			List<Genre> listaFinal = new ArrayList<Genre>();
			
			for (Map.Entry<String, Map<String, Integer>> entry: genreMap.entrySet()) {
				if (entry != null) {
					for (Map.Entry<String, Integer> value: entry.getValue().entrySet()) {
						if (value != null) {
							// System.out.println(entry.getKey()+" - "+value.getKey()+" - "+value.getValue());
							Genre listElement = new Genre();
							listElement.setGenre(entry.getKey());
							listElement.setCountry(value.getKey());
							listElement.setStreams(value.getValue());
							listaFinal.add(listElement);							
						}
					}
				}
			}
			
			beanToCsv.write(listaFinal);
			
			writer.close();
			
			log.debug("Proceso finalizado");
		}
		
	}

}
