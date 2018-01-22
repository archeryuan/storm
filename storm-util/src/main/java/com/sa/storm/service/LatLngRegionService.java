/**
 * 
 */
package com.sa.storm.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sa.common.domain.Region;
import com.sa.common.hbase.service.HBaseCountryCityService;
import com.sa.common.json.JsonUtil;
import com.sa.common.service.RegionService;
import com.sa.common.service.TranslationService;
import com.sa.common.util.HttpClientUtil;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.framework.App;

/**
 * @author lewis
 * 
 */
public class LatLngRegionService {

	private static final Logger log = LoggerFactory.getLogger(LatLngRegionService.class);

	private static final String CLIENT_ID = "gme-masterconceptinternational";
	private static final String CACHE_PREFIX = "rgcc-";
	private static final String URL_PREFIX = "http://maps.googleapis.com/maps/api/geocode/json?sensor=true";
	private static LatLngRegionService instance;

	private final ExecutorService eService;
	private final RegionService regionService;
	private final ObjectMapper objectMapper;

	private LatLngRegionService() {
		eService = Executors.newSingleThreadExecutor();
		regionService = App.getInstance().getContext().getBean(RegionService.class);
		objectMapper = JsonUtil.getMapper();
	}

	public static LatLngRegionService getInstance() {
		if (instance == null) {
			synchronized (LatLngRegionService.class) {
				if (instance == null) {
					instance = new LatLngRegionService();
				}
			}
		}
		return instance;
	}

	// public static void main(String[] args) throws ClientProtocolException, IOException {
	// Region region = RegionService.getInstance().reverseGeocode("24.94236247,121.37850791");
	// log.info("Region: {}", region.toString());
	//
	// ObjectMapper mapper = new ObjectMapper();
	// String s = mapper.writeValueAsString(region);
	// log.info(s);
	// region = mapper.readValue(s, Region.class);
	// log.info("Region: {}", region.toString());
	// }

	private void setCache(final Region region, final String latlong) {
		if (region == null || latlong == null)
			return;

		eService.execute(new Runnable() {
			@Override
			public void run() {
				Jedis jedis = null;

				try {
					String value = objectMapper.writeValueAsString(region);
					jedis = RedisUtil.getInstance().getResource();
					String key = CACHE_PREFIX + latlong;
					jedis.setex(key, 60 * 60 * 48, value);
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				} finally {
					if (jedis != null) {
						try {
							RedisUtil.getInstance().returnResource(jedis);
						} catch (Exception e) {
						}
					}
				}
			}
		});
	}

	private Region lookUpCache(String latlong) {
		String key = CACHE_PREFIX + latlong;
		Jedis jedis = null;

		try {
			jedis = RedisUtil.getInstance().getResource();
			String str = jedis.get(key);
			if (str != null && str.length() > 0) {
				return objectMapper.readValue(str, Region.class);
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			if (jedis != null) {
				try {
					RedisUtil.getInstance().returnResource(jedis);
				} catch (Exception e) {
				}
			}
		}

		return null;
	}

	protected String getFlagKey() throws UnknownHostException {
		return RedisDefinition.RegionServiceDef.KEY_LIMIT_PREFIX + InetAddress.getLocalHost().getHostAddress();
	}

	protected boolean checkApiLimit() {
		Jedis jedis = null;

		try {
			String key = getFlagKey();
			jedis = RedisUtil.getInstance().getResource();
			String value = jedis.get(key);

			if (value != null && Long.parseLong(value) > 2500) {
				return false;
			} else {
				jedis.watch(key);
				Transaction t = jedis.multi();
				Response<Long> reply = t.incr(key);
				t.exec();

				if (reply.get() == 1) {
					jedis.expire(key, 60 * 60 * 24);
				}

				return true;
			}
		} catch (UnknownHostException e) {
			log.error(e.getMessage(), e);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}

		return false;
	}

	/**
	 * Look up region with latitude, longitude
	 * 
	 * @param latitude
	 * @param longitude
	 * @return
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public Region reverseGeocode(double latitude, double longitude) throws ClientProtocolException, IOException {

		String latlong = latitude + "," + longitude;

		// Try get from redis first
		Region region = lookUpCache(latlong);
		if (region != null) {
			return region;
		}

		final StringBuilder uri = new StringBuilder();
		uri.append(URL_PREFIX);
		// uri.append("&client=").append(CLIENT_ID);
		uri.append("&latlng=").append(latlong);
		log.debug(uri.toString());

		HttpGet get = null;
		String content = null;

		if (!checkApiLimit()) {
			return null;
		}

		try {
			get = new HttpGet(uri.toString());
			HttpClient httpClient = HttpClientUtil.getInstance().getClient();
			HttpEntity entity = httpClient.execute(get).getEntity();
			content = EntityUtils.toString(entity);
			log.debug("Received JSON from map API: {}", content);

			EntityUtils.consumeQuietly(entity);
		} catch (ParseException e) {
			log.error(e.getMessage(), e);
		} finally {
			if (get != null) {
				get.releaseConnection();
			}
		}

		region = loadRegion(content);

		if (region != null)
			setCache(region, latlong);

		return region;
	}

	private Region loadRegion(String jsonStr) {
		if (StringUtils.isBlank(jsonStr))
			return null;

		final JSONObject rootJson = (JSONObject) JSONValue.parse(jsonStr);
		Region dbRegion = null;

		try {
			if (rootJson.containsKey("results") && rootJson.get("results") != null) {
				final HBaseCountryCityService ccService = new HBaseCountryCityService();
				final JSONArray jsonArray = (JSONArray) rootJson.get("results");
				final List<Region> regionList = new ArrayList<Region>();
				// final List<String> regionNames = new ArrayList<String>();
				Region region = null, country = null;
				// region.setParentId(0L);

				for (int i = 0; i < jsonArray.size(); i++) {
					final JSONObject result = (JSONObject) jsonArray.get(i);
					JSONArray types = (JSONArray) result.get("types");

					if (types.contains("locality")) {
						final JSONArray addrComponents = (JSONArray) result.get("address_components");
						JSONObject admin1Json = null, admin2Json = null, localityJson = null, countryJson = null;
						boolean countryIsCity = false, admin1IsCity = false, admin2IsCity = false;

						countryJson = getAddressComponent("country", addrComponents);
						admin1Json = getAddressComponent("administrative_area_level_1", addrComponents);
						admin2Json = getAddressComponent("administrative_area_level_2", addrComponents);
						localityJson = getAddressComponent("locality", addrComponents);

						country = new Region();
						country.setCountryEngName((String) countryJson.get("long_name"));
						country.setCountryCode((String) countryJson.get("short_name"));
						country.setRegionEngName(country.getCountryEngName());
						countryIsCity = ccService.isCity(country.getCountryCode(), country.getRegionEngName());
						if (countryIsCity) {
							regionList.add(country);
						}

						if (admin1Json != null && !countryIsCity) {
							region = new Region();
							region.setCountryCode(country.getCountryCode());
							region.setCountryEngName(country.getCountryEngName());
							region.setRegionEngName((String) admin1Json.get("long_name"));
							regionList.add(region);
							admin1IsCity = ccService.isCity(region.getCountryCode(), region.getRegionEngName());
						}

						if (admin2Json != null && !countryIsCity && !admin1IsCity) {
							region = new Region();
							region.setCountryCode(country.getCountryCode());
							region.setCountryEngName(country.getCountryEngName());
							region.setRegionEngName((String) admin2Json.get("long_name"));
							regionList.add(region);
							admin2IsCity = ccService.isCity(region.getCountryCode(), region.getRegionEngName());
						}

						if (localityJson != null && !countryIsCity && !admin1IsCity && !admin2IsCity) {
							region = new Region();
							region.setCountryCode(country.getCountryCode());
							region.setCountryEngName(country.getCountryEngName());
							region.setRegionEngName((String) localityJson.get("long_name"));
							regionList.add(region);
						}

						break;
					}
				}

				CollectionUtils.forAllDo(regionList, new Closure() {
					@Override
					public void execute(Object input) {
						Region region = (Region) input;
						correctRegion(region);
					}
				});

				if (regionList.size() > 0) {
					Region lastRegion = regionList.get(regionList.size() - 1);
					dbRegion = regionService.searchRegion(lastRegion.getCountryCode(), lastRegion.getRegionEngName(), null);
					if (dbRegion == null) {
						// Region does not exists, do update region tree
						List<Region> dbRegionList = updateRegionTree(regionList);

						// return last region in list
						if (dbRegionList.size() > 0) {
							log.debug("Regions: {}", dbRegionList);
							return dbRegionList.get(dbRegionList.size() - 1);
						}
					}
				} else {
					log.info("Region result array: {}", jsonArray);
				}

				return dbRegion;
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		return null;
	}

	private void correctRegion(Region region) {
		if (region == null)
			return;

		if (region.getCountryCode().equalsIgnoreCase("hk") && region.getRegionEngName().equalsIgnoreCase("hong kong")) {
			region.setCountryCode("CN");
			return;
		}

		if (region.getRegionEngName().equalsIgnoreCase("macau")) {
			region.setCountryCode("CN");
			return;
		}

		// if (region.getRegionEngName().equalsIgnoreCase("taiwan")) {
		// region.setCountryCode("TW");
		// return;
		// }

		if (region.getCountryCode().equalsIgnoreCase("cn") && region.getRegionEngName().equalsIgnoreCase("Ma'anshan")) {
			region.setRegionEngName("Ma On Shan");
			return;
		}

		if (region.getCountryCode().equalsIgnoreCase("cn") && region.getRegionEngName().equalsIgnoreCase("Ngari")) {
			region.setRegionEngName("Ali");
			return;
		}

		if (region.getCountryCode().equalsIgnoreCase("cn") && region.getRegionEngName().equalsIgnoreCase("Nei Mongol")) {
			region.setRegionEngName("Nei Monggol");
			return;
		}

		if (region.getRegionEngName().equalsIgnoreCase("Zhangzhou Zhi")) {
			region.setRegionEngName("Zhangzhou");
			return;
		}

		if (region.getRegionEngName().equalsIgnoreCase("Nagqu")) {
			region.setRegionEngName("The Nagqu Prefecture");
			return;
		}

		if (region.getRegionEngName().equalsIgnoreCase("Xizang (Tibet)")) {
			region.setRegionEngName("Xizang");
			return;
		}

		if (region.getRegionEngName().equalsIgnoreCase("Rikaze")) {
			region.setRegionEngName("Lhasa");
			return;
		}
	}

	@Transactional
	private List<Region> updateRegionTree(List<Region> regions) {
		final List<Region> dbRegions = new ArrayList<Region>();
		Region dbRegion = null;

		for (Region region : regions) {
			if (dbRegion != null) {
				region.setParentId(dbRegion.getId());
				region.setCountryChiName(dbRegion.getCountryChiName());
			}

			dbRegion = insertIfNotPresent(region);
			dbRegions.add(dbRegion);
		}

		return dbRegions;
	}

	private Region insertIfNotPresent(Region region) {
		Region dbRegion = null;

		dbRegion = regionService.searchRegion(region.getCountryCode(), region.getRegionEngName(), null);
		if (dbRegion == null) {
			translateRegion(region);
			return regionService.addRegion(region);
		}

		return null;
	}

	private void translateRegion(Region region) {
		// TranslationService.getInstance().translateBatch();
		if (region.getCountryChiName() == null) {
			region.setCountryChiName(TranslationService.getInstance().translate(region.getCountryEngName()));
		}
		if (region.getRegionChiName() == null) {
			region.setRegionChiName(TranslationService.getInstance().translate(region.getRegionEngName()));
		}
		if (region.getRegionChiName() == null) {
			region.setRegionChiName(TranslationService.getInstance().translate(StringUtils.remove(region.getRegionEngName(), '\'')));
		}
	}

	// @Deprecated
	// private void insertRegion(Region region, Connection conn) throws Exception {
	// log.info("Inserting region: {}", region.toString());
	//
	// int count = 1;
	// final StringBuilder query = new StringBuilder();
	// query.append("insert into Region set countryCode = ?, ");
	// query.append("regionEngName = ?, regionChiName = ?, countryEngName = ?, countryChiName = ?");
	// if (region.getParentId() != null)
	// query.append(", parentId = ?");
	// if (region.getRegionId() != null)
	// query.append(", regionId = ?");
	// query.append(", id = ?");
	//
	// PreparedStatement ps = null;
	// try {
	// long nextId = getNextId(conn);
	// ps = conn.prepareStatement(query.toString());
	// ps.setString(count++, region.getCountryCode());
	// ps.setString(count++, region.getRegionEngName());
	// ps.setString(count++, region.getRegionChiName());
	// ps.setString(count++, region.getCountryEngName());
	// ps.setString(count++, region.getCountryChiName());
	//
	// if (region.getParentId() != null)
	// ps.setLong(count++, region.getParentId());
	//
	// if (region.getRegionId() != null)
	// ps.setString(count++, region.getRegionId());
	//
	// ps.setLong(count++, nextId);
	//
	// ps.executeUpdate();
	// } catch (Exception e) {
	// throw e;
	// } finally {
	// if (ps != null) {
	// try {
	// ps.close();
	// } catch (Exception e) {
	// }
	// }
	// }
	// }

	// @Deprecated
	// private long getNextId(Connection conn) throws Exception {
	// String query = "select max(id) from Region";
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	// try {
	// ps = conn.prepareStatement(query);
	// rs = ps.executeQuery();
	// if (rs.next()) {
	// long value = rs.getLong(1);
	// return ++value;
	// } else {
	// throw new Exception("Failed to get next record id.");
	// }
	// } catch (SQLException e) {
	// throw e;
	// } finally {
	// if (rs != null)
	// try {
	// rs.close();
	// } catch (SQLException e) {
	// }
	// if (ps != null)
	// try {
	// ps.close();
	// } catch (SQLException e) {
	// }
	// }
	// }

	private JSONObject getAddressComponent(String key, JSONArray addrComponents) {
		if (addrComponents == null) {
			return null;
		}

		JSONArray types;
		JSONObject addrCom;

		for (int i = 0; i < addrComponents.size(); i++) {
			addrCom = (JSONObject) addrComponents.get(i);
			types = (JSONArray) addrCom.get("types");
			if (types.contains(key)) {
				return addrCom;
			}
		}

		return null;
	}

	// private Region searchRegion(String countryCode, String regionName, Long parentId, Connection conn) throws Exception {
	//
	// if (countryCode == null || regionName == null) {
	// return null;
	// }
	//
	// final StringBuilder query = new StringBuilder();
	//
	// query.append("select * from Region where countryCode = ? and LOWER(regionEngName) = ?");
	// if (parentId != null) {
	// query.append(" and parentId = ?");
	// }
	// log.debug("Query: {}", query.toString());
	//
	// Region region = null;
	// int counter = 1;
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	//
	// try {
	// ps = conn.prepareStatement(query.toString());
	// ps.setString(counter++, countryCode);
	// ps.setString(counter++, regionName.toLowerCase());
	// if (parentId != null) {
	// ps.setLong(counter, parentId);
	// }
	//
	// rs = ps.executeQuery();
	// region = rsToRegion(rs);
	// } catch (Exception e) {
	// log.error("Failed querying database Region table.");
	// throw e;
	// } finally {
	// if (rs != null) {
	// rs.close();
	// }
	// if (ps != null) {
	// ps.close();
	// }
	// }
	//
	// return region;
	// }

	// private Region searchRegion(String countryCode, String regionName, Long parentId) throws Exception {
	//
	// if (countryCode == null || regionName == null) {
	// return null;
	// }
	//
	// final StringBuilder query = new StringBuilder();
	// final MySQLUtil connManager = new MySQLUtil();
	//
	// query.append("select * from Region where countryCode = ? and LOWER(regionEngName) = ?");
	// if (parentId != null) {
	// query.append(" and parentId = ?");
	// }
	// log.debug("Query: {}", query.toString());
	//
	// Region region = null;
	// int counter = 1;
	// Connection conn = null;
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	//
	// try {
	// conn = connManager.getConnection();
	// ps = conn.prepareStatement(query.toString());
	// ps.setString(counter++, countryCode);
	// ps.setString(counter++, regionName.toLowerCase());
	// if (parentId != null) {
	// ps.setLong(counter, parentId);
	// }
	//
	// rs = ps.executeQuery();
	// region = rsToRegion(rs);
	// } catch (Exception e) {
	// log.error("Failed querying database Region table.");
	// throw e;
	// } finally {
	// if (rs != null) {
	// rs.close();
	// }
	// if (ps != null) {
	// ps.close();
	// }
	// if (conn != null) {
	// connManager.closeConnection();
	// }
	// }
	//
	// return region;
	// }

	// private List<Region> searchRegion(String countryCode, String[] regions) throws Exception {
	// if (countryCode == null || regions == null) {
	// return null;
	// }
	//
	// final StringBuilder query = new StringBuilder();
	// final MySQLUtil connManager = new MySQLUtil();
	//
	// query.append("select * from Region where countryCode = ? and LOWER(regionEngName) in (");
	// for (int i = 0; i < regions.length; i++) {
	// if (i > 0) {
	// query.append(",?");
	// } else {
	// query.append("?");
	// }
	// }
	// query.append(") order by ").append(Region.COL_ID);
	// log.debug("Query: {}", query.toString());
	//
	// List<Region> regionList = null;
	// int counter = 1;
	// Connection conn = null;
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	// try {
	// conn = connManager.getConnection();
	// ps = conn.prepareStatement(query.toString());
	// ps.setString(counter++, countryCode);
	//
	// for (String regionName : regions) {
	// ps.setString(counter++, regionName.toLowerCase());
	// }
	//
	// rs = ps.executeQuery();
	// regionList = rsToRegionList(rs);
	// } catch (Exception e) {
	// log.error("Failed querying database Region table.");
	// throw e;
	// } finally {
	// if (rs != null) {
	// rs.close();
	// }
	// if (ps != null) {
	// ps.close();
	// }
	// if (conn != null) {
	// connManager.closeConnection();
	// }
	// }
	//
	// return regionList;
	// }

	// private Region rsToRegion(ResultSet rs) throws SQLException {
	// if (rs == null) {
	// return null;
	// }
	//
	// Region region = null;
	//
	// if (rs.next()) {
	// region = new Region();
	// region.setCountryChiName(rs.getString(Region.COL_COUNTRY_CHI_NAME));
	// region.setCountryCode(rs.getString(Region.COL_COUNTRY_CODE));
	// region.setCountryEngName(rs.getString(Region.COL_COUNTRY_ENG_NAME));
	// region.setId(rs.getLong(Region.COL_ID));
	// region.setParentId(rs.getLong(Region.COL_PARENT_ID));
	// region.setRegionChiName(rs.getString(Region.COL_REGION_CHI_NAME));
	// region.setRegionEngName(rs.getString(Region.COL_REGION_ENG_NAME));
	// region.setRegionId(rs.getString(Region.COL_REGION_ID));
	// }
	//
	// return region;
	// }

	// /**
	// * Convert sql ResultSet to list of Region object
	// *
	// * @param rs
	// * @return
	// * @throws SQLException
	// */
	// private List<Region> rsToRegionList(ResultSet rs) throws SQLException {
	// final List<Region> regionList = new ArrayList<Region>();
	//
	// if (rs == null) {
	// return regionList;
	// }
	//
	// Region region = null;
	//
	// while (rs.next()) {
	// region = new Region();
	// region.setCountryChiName(rs.getString(Region.COL_COUNTRY_CHI_NAME));
	// region.setCountryCode(rs.getString(Region.COL_COUNTRY_CODE));
	// region.setCountryEngName(rs.getString(Region.COL_COUNTRY_ENG_NAME));
	// region.setId(rs.getLong(Region.COL_ID));
	// region.setParentId(rs.getLong(Region.COL_PARENT_ID));
	// region.setRegionChiName(rs.getString(Region.COL_REGION_CHI_NAME));
	// region.setRegionEngName(rs.getString(Region.COL_REGION_ENG_NAME));
	// region.setRegionId(rs.getString(Region.COL_REGION_ID));
	//
	// regionList.add(region);
	// }
	//
	// return regionList;
	// }
}
