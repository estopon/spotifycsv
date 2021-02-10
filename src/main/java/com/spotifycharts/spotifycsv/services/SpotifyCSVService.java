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
		
		Writer writer = Files.newBufferedWriter(Paths.get(filesDirectory+outputFilename));
		StatefulBeanToCsv<Genre> beanToCsv = new StatefulBeanToCsvBuilder<Genre>(writer)
                .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                .build();
		
		for (String country: countryList) {
			
			String urlStr = chartsUrl.replace("$1", country);
			
			LocalDate startDate = LocalDate.now().minusDays(Long.parseLong(numDays)+1);
			LocalDate endDate = LocalDate.now().minusDays(1);
			
			List<ChartElement> completeList = new ArrayList<ChartElement>();
			
			while (startDate.isBefore(endDate)) {
				
				String dateStr = formatter.format(Date.from(startDate.atStartOfDay(defaultZoneId).toInstant()));
				
				urlStr = urlStr.replace("$2", dateStr);
				
				try {
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
					
					SpotifyApi spotifyApi = new SpotifyApi.Builder()
						    .setClientId(clientID)
						    .setClientSecret(clientSecret)
						    .build();
					
					ClientCredentialsRequest clientCredentialsRequest = spotifyApi.clientCredentials()
						    .build();
					
					ClientCredentials clientCredentials = clientCredentialsRequest.execute();
					
					spotifyApi.setAccessToken(clientCredentials.getAccessToken());
					
					System.out.println("Token "+clientCredentials.getAccessToken()+ " expires in: " + clientCredentials.getExpiresIn());
					
					for (ChartElement chart: charts) {
						
						System.out.println(country + " - " + dateStr + " - " + chart.getPosition() + "/" + charts.size());
						
						String trackID = chart.getUrl();
						int index=trackID.lastIndexOf('/');
						trackID = trackID.substring(index+1);
						
						// GetAudioFeaturesForTrackRequest getAudioFeaturesForTrackRequest = spotifyApi.getAudioFeaturesForTrack(trackID).build();
						// AudioFeatures audioFeatures = getAudioFeaturesForTrackRequest.execute();
						
						chart.setCountry(country);
						chart.setDate(dateStr);
						
						try {
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
							
							Thread.sleep(100);
						} catch (Exception e) {
							log.error("--> Exception charts loop: "+e.getMessage(), e);
							ArrayList<String> genres = new ArrayList<String>();
							genres.add("error");
							chart.setGenres(genres);
						}
						
					}
					
					completeList.addAll(charts);
					
					fileReader.close();
					
					Path filePath = Paths.get(fileName);
					Files.delete(filePath);	
				
				} catch (Exception e) {
					log.error("--> Exception "+country+"-"+dateStr+": "+e.getMessage(), e);
				} finally {
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
						
						String mainGenre;
						
						if (genreStr.toLowerCase().equals("metal") ||
							genreStr.toLowerCase().equals("metalcore") ||
							genreStr.toLowerCase().indexOf(" metal") != -1 ||
							genreStr.toLowerCase().indexOf(" metalcore") != -1) {
							mainGenre = "metal";
						} else if (genreStr.toLowerCase().equals("rock") ||
							       genreStr.toLowerCase().indexOf(" rock") != -1) {
							mainGenre = "rock";
						} else if (genreStr.toLowerCase().equals("pop") ||
								   genreStr.toLowerCase().equals("electropop") ||
								   genreStr.toLowerCase().equals("synthpop") ||
								   genreStr.toLowerCase().equals("scandipop") ||
								   genreStr.toLowerCase().indexOf("-pop") != -1 ||
								   genreStr.toLowerCase().indexOf(" pop") != -1 ||
								   genreStr.toLowerCase().indexOf("pop ") != -1) {
							mainGenre = "pop";
						} else if (genreStr.toLowerCase().indexOf("indie") != -1) {
							mainGenre = "indie";
						} else if (genreStr.toLowerCase().indexOf("hip hop") != -1) {
							mainGenre = "hip hop";
						} else if (genreStr.toLowerCase().equals("rap") ||
								   genreStr.toLowerCase().indexOf(" rap") != -1 ||
								   genreStr.toLowerCase().indexOf("rap ") != -1) {
							mainGenre = "rap";
						} else if (genreStr.toLowerCase().equals("r&b") ||
								   genreStr.toLowerCase().indexOf(" r&b") != -1 ||
								   genreStr.toLowerCase().indexOf("r&b ") != -1) {
							mainGenre = "r&b";
						} else if (genreStr.toLowerCase().equals("trap") ||
								   genreStr.toLowerCase().indexOf(" trap") != -1 ||
								   genreStr.toLowerCase().indexOf("trap ") != -1) {
							mainGenre = "trap";
						} else if (genreStr.toLowerCase().equals("dance") ||
								   genreStr.toLowerCase().indexOf(" dance") != -1 ||
								   genreStr.toLowerCase().indexOf("dance ") != -1) {
							mainGenre = "dance";
						} else if (genreStr.toLowerCase().equals("house") ||
								   genreStr.toLowerCase().indexOf(" house") != -1 ||
								   genreStr.toLowerCase().indexOf("house ") != -1) {
							mainGenre = "house";
						} else if (genreStr.toLowerCase().indexOf("grunge") != -1) {
							mainGenre = "grunge";
						} else if (genreStr.toLowerCase().equals("country") ||
								   genreStr.toLowerCase().indexOf(" country") != -1 ||
								   genreStr.toLowerCase().indexOf("country ") != -1) {
							mainGenre = "country";
						} else if (genreStr.toLowerCase().equals("latin") ||
								   genreStr.toLowerCase().indexOf(" latin") != -1 ||
								   genreStr.toLowerCase().indexOf("latin ") != -1) {
							mainGenre = "latin";
						} else if (genreStr.toLowerCase().equals("reggaeton") ||
								   genreStr.toLowerCase().indexOf(" reggaeton") != -1 ||
								   genreStr.toLowerCase().indexOf("reggaeton ") != -1) {
							mainGenre = "reggaeton";
						} else {
							mainGenre = "others";
						}
						genre.setMainGenre(mainGenre);
						
						genreList.add(genre);
					}				
				}
				
				Map<String, Map<String, Map<String, Integer>>> genreMap = genreList.stream().collect(
					Collectors.groupingBy(
							Genre::getGenre,
							Collectors.groupingBy(
									Genre::getCountry,
									Collectors.groupingBy(
											Genre::getMainGenre,
											Collectors.mapping(
													Genre::getStreams,
													Collectors.reducing(0, a -> a, (a1, a2) -> a1 + a2)
											)
									)						
							)				
					)	
				);
				
				List<Genre> listaFinal = new ArrayList<Genre>();
				
				for (Map.Entry<String, Map<String, Map<String, Integer>>> entry: genreMap.entrySet()) {
					if (entry != null) {
						for (Map.Entry<String, Map<String, Integer>> entry2: entry.getValue().entrySet()) {
							if (entry2 != null) {
								for (Map.Entry<String, Integer> value: entry2.getValue().entrySet()) {
									if (value != null) {
										// System.out.println(entry.getKey()+" - "+value.getKey()+" - "+value.getValue());
										Genre listElement = new Genre();
										listElement.setGenre(entry.getKey());
										listElement.setCountry(entry2.getKey());
										listElement.setMainGenre(value.getKey());									
										listElement.setStreams(value.getValue());
										listaFinal.add(listElement);							
									}
								}
							}				
						}
					}
				}
				
				beanToCsv.write(listaFinal);
				
			}
			
		}
		
		writer.close();
		
		log.debug("Proceso finalizado");
		
	}

}
