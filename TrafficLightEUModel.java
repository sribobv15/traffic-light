package com.samplevechile.ibis.model;  
/*
 * @Srinivas
 * @bobba
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.samplevechile.ibis.common.Constant;
import com.f.samplevechile.common.IbisSession;
import com.samplevechile.ibis.db.SqlUtils;
import com.samplevechile.ibis.enumerations.Continent;
import com.samplevechile.ibis.enumerations.ProductHierarchy;
import com.samplevechile.ibis.exception.IbisRuntimeException;
import com.samplevechile.ibis.inbound.layer.base.BaseModel;
import com.samplevechile.ibis.record.AllMarkets;
import com.samplevechile.ibis.record.DailySalesPerformanceEURate;
import com.samplevechile.ibis.record.DailySalesPerformanceEURecord;
import com.samplevechile.ibis.record.ProductHierarchyRecord;
import com.samplevechile.ibis.record.TrafficLightCurrencyTypes;
import com.samplevechile.ibis.record.TrafficLightMetricTypes;
import com.samplevechile.it.exception.FordExceptionAttributes;
import com.samplevechile.it.logging.ILogger;
import com.samplevechile.it.logging.Level;
import com.samplevechile.it.logging.LogFactory;
import com.ibm.icu.text.DecimalFormat;
public class TrafficLightEUModel extends BaseModel {
	private static final String CLASS_NAME = TrafficLightEUModel.class.getName();
	private static final ILogger log = LogFactory.getInstance().getLogger(CLASS_NAME);
	public HashMap <String,HashMap <String,Integer>> hmapObjectiveLevels;
	public HashMap<String,String> hMapDealerChannelMap;
	public static boolean displayWarrantyForGrandTotal = true;
	public TrafficLightEUModel() {
		log.log(Level.FINER, CLASS_NAME);
		this.hmapObjectiveLevels = buildObjectiveLevels();
		loadCustomerMap();
	}

	/**
	 * @return HashMap
	 */
	private HashMap<String,String> loadCustomerMap(){
		final String METHOD_NAME = "loadCustomerMap()";
		log.entering(CLASS_NAME, METHOD_NAME);

		hMapDealerChannelMap = new HashMap<String, String>(); 	

		hMapDealerChannelMap.put("1","Dealer");
		hMapDealerChannelMap.put("2","Parts Plus");
		hMapDealerChannelMap.put("3","Powertrain Dist");    	
		hMapDealerChannelMap.put("4","FCS");
		hMapDealerChannelMap.put("5","Special Mkts");
		hMapDealerChannelMap.put("6","Joint Venture");
		hMapDealerChannelMap.put("7","Strategic Cust");
		hMapDealerChannelMap.put("8","Fleets");
		hMapDealerChannelMap.put("9","FMC");
		hMapDealerChannelMap.put("A","Other");
		hMapDealerChannelMap.put("B","Special Cust");//for release-1 we made this change

		log.exiting(CLASS_NAME, METHOD_NAME);	
		return hMapDealerChannelMap;    	

	}


	public HashMap <String,HashMap <String,Integer>> buildObjectiveLevels(){

		final String  METHOD_NAME = "buildObjectiveLevels";
		log.entering(CLASS_NAME, METHOD_NAME);

		HashMap <String,HashMap <String,Integer>> hmapObjectiveLevels = new HashMap<String, HashMap<String,Integer>>();


		HashMap<String,Integer> hmapDEUObjectives = new HashMap<String,Integer>();
		hmapDEUObjectives.put("DDEU1", 5);
		hmapDEUObjectives.put("OTHER", 5);


		HashMap<String,Integer> hmapGBRObjectives = new HashMap<String,Integer>();
		hmapGBRObjectives.put("DGBR1", 5);
		hmapGBRObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapESPObjectives = new HashMap<String,Integer>();
		hmapESPObjectives.put("DESP1", 5);
		hmapESPObjectives.put("OTHER", 5);


		HashMap<String,Integer> hmapFRAObjectives = new HashMap<String,Integer>();
		hmapFRAObjectives.put("DFRA1", 5);
		hmapFRAObjectives.put("OTHER", 5);


		HashMap<String,Integer> hmapITAObjectives = new HashMap<String,Integer>();
		hmapITAObjectives.put("DITA1", 5);
		hmapITAObjectives.put("OTHER", 5);


		HashMap<String,Integer> hmapAUTObjectives = new HashMap<String,Integer>();
		hmapAUTObjectives.put("DAUT1", 5);
		hmapAUTObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapBELObjectives = new HashMap<String,Integer>();
		hmapBELObjectives.put("DBEL1", 5);
		hmapBELObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapCZEObjectives = new HashMap<String,Integer>();
		hmapCZEObjectives.put("DCZE1", 5);
		hmapCZEObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapCHEObjectives = new HashMap<String,Integer>();
		hmapCHEObjectives.put("DCHE1", 5);
		hmapCHEObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapDNKObjectives = new HashMap<String,Integer>();
		hmapDNKObjectives.put("DDNK1", 5);
		hmapDNKObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapEDMObjectives = new HashMap<String,Integer>();
		hmapEDMObjectives.put("DEDM1", 5);
		hmapEDMObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapFINObjectives = new HashMap<String,Integer>();
		hmapFINObjectives.put("DFIN1", 5);
		hmapFINObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapGRCObjectives = new HashMap<String,Integer>();
		hmapGRCObjectives.put("DGRC1", 5);
		hmapGRCObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapHUNObjectives = new HashMap<String,Integer>();
		hmapHUNObjectives.put("DHUN1", 5);
		hmapHUNObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapIRLObjectives = new HashMap<String,Integer>();
		hmapIRLObjectives.put("DIRL1", 5);
		hmapIRLObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapNLDObjectives = new HashMap<String,Integer>();
		hmapNLDObjectives.put("DNLD1", 5);
		hmapNLDObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapNORObjectives = new HashMap<String,Integer>();
		hmapNORObjectives.put("DNOR1", 5);
		hmapNORObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapPRTObjectives = new HashMap<String,Integer>();
		hmapPRTObjectives.put("DPRT1", 5);
		hmapPRTObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapPOLObjectives = new HashMap<String,Integer>();
		hmapPOLObjectives.put("DPOL1", 5);
		hmapPOLObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapRUSObjectives = new HashMap<String,Integer>();
		hmapRUSObjectives.put("DRUS1", 5);
		hmapRUSObjectives.put("OTHER", 5);


		HashMap<String,Integer> hmapROMObjectives = new HashMap<String,Integer>();
		hmapROMObjectives.put("DROM1", 5);
		hmapROMObjectives.put("OTHER", 5);

		HashMap<String,Integer> hmapSWEObjectives = new HashMap<String,Integer>();
		hmapSWEObjectives.put("DSWE1",5);
		hmapSWEObjectives.put("OTHER",5);

		HashMap<String,Integer> hmapTURObjectives = new HashMap<String,Integer>();
		hmapTURObjectives.put("DTUR1",5);
		hmapTURObjectives.put("OTHER",5);

		HashMap<String,Integer> hmapJVCObjectives = new HashMap<String,Integer>();
		hmapJVCObjectives.put("DJVC1",5);
		hmapJVCObjectives.put("OTHER",5);

		hmapObjectiveLevels.put("DEU",hmapDEUObjectives);
		hmapObjectiveLevels.put("GBR",hmapGBRObjectives);
		hmapObjectiveLevels.put("ESP",hmapESPObjectives);
		hmapObjectiveLevels.put("FRA",hmapFRAObjectives);
		hmapObjectiveLevels.put("ITA",hmapITAObjectives);
		hmapObjectiveLevels.put("AUT",hmapAUTObjectives);
		hmapObjectiveLevels.put("BEL",hmapBELObjectives);
		hmapObjectiveLevels.put("CZE",hmapCZEObjectives);
		hmapObjectiveLevels.put("CHE",hmapCHEObjectives);
		hmapObjectiveLevels.put("DNK",hmapDNKObjectives);
		hmapObjectiveLevels.put("EDM",hmapEDMObjectives);
		hmapObjectiveLevels.put("FIN",hmapFINObjectives);
		hmapObjectiveLevels.put("GRC",hmapGRCObjectives);
		hmapObjectiveLevels.put("HUN",hmapHUNObjectives);
		hmapObjectiveLevels.put("IRL",hmapIRLObjectives);
		hmapObjectiveLevels.put("NLD",hmapNLDObjectives);
		hmapObjectiveLevels.put("NOR",hmapNORObjectives);
		hmapObjectiveLevels.put("PRT",hmapPRTObjectives);
		hmapObjectiveLevels.put("POL",hmapPOLObjectives);
		hmapObjectiveLevels.put("RUS",hmapRUSObjectives);
		hmapObjectiveLevels.put("ROM",hmapROMObjectives);
		hmapObjectiveLevels.put("SWE",hmapSWEObjectives);
		hmapObjectiveLevels.put("TUR",hmapTURObjectives);
		hmapObjectiveLevels.put("JVC",hmapJVCObjectives);


		log.exiting(CLASS_NAME, METHOD_NAME);
		return hmapObjectiveLevels;

	}

	/**
	 * @param strRegion
	 * @param strMarket
	 * @param strMonthYr
	 * @return String
	 */
	public String getChannelsWithoutObjectives(String strRegion, String strMarket, String strMonthYr){
		final String  METHOD_NAME = "getChannelsWithoutObjectives(String,String,String)";
		log.entering(CLASS_NAME, METHOD_NAME);
		String strChannels = "";
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = getConnectionFromFactory();
			StringBuffer sqlString = new StringBuffer();
			/*SELECT DISTINCT MRKT_MARKET_C channel 
		 FROM GFIN_DATA.SGFDG50 
				WHERE GFDA07_REGION_C = 'eu' 
				AND  GFDG50_DSPLY_CTRY_C = 'gbr' 
				AND  GFDG50_MNTHYR_Y = 201810
				AND MRKT_MARKET_C NOT IN ( 
				SELECT DISTINCT  MRKT_MARKET_C 
			 FROM GFIN_DATA.SGFDG51 
				WHERE GFDA07_REGION_C = 'eu' 
			AND  GFDG51_MARKET_C = 'gbr' 
				AND  GFDG51_MNTHYR_Y = 201810 ) */
			sqlString.append("SELECT DISTINCT MRKT_MARKET_C channel ");
			sqlString.append(" FROM " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
			sqlString.append("	WHERE GFDA07_REGION_C = ? ");
			sqlString.append("	AND  GFDG50_DSPLY_CTRY_C = ? ");
			sqlString.append("	AND  GFDG50_MNTHYR_Y = ? ");
			sqlString.append("	AND MRKT_MARKET_C NOT IN ( ");
			sqlString.append("	SELECT DISTINCT  MRKT_MARKET_C ");
			sqlString.append(" FROM "+ SqlUtils.getDbInstanceView() + ".SGFDG51 ");
			sqlString.append("	WHERE GFDA07_REGION_C = ? ");
			sqlString.append("	AND  GFDG51_MARKET_C = ? ");
			sqlString.append("	AND  GFDG51_MNTHYR_Y = ? ) ");
			log.logp(Level.INFO,METHOD_NAME,"Input Params --",strRegion + strMarket + ", " + strMonthYr);
			log.logp(Level.INFO,METHOD_NAME,"Query --",sqlString.toString());
			pstmt = con.prepareStatement(sqlString.toString());
			pstmt.setString(1, strRegion);
			pstmt.setString(2, strMarket);
			pstmt.setString(3, strMonthYr);
			pstmt.setString(4, strRegion);
			pstmt.setString(5, strMarket);
			pstmt.setString(6, strMonthYr);
			StringBuffer strBufferChannels = new StringBuffer();
			rs = pstmt.executeQuery();
			Set<String> lastCharacterValue = new HashSet();
			while (rs.next()) {
				String tempChannel = rs.getString("channel").trim();				 
				String lastCharacter = tempChannel.substring(tempChannel.length()-1, tempChannel.length());				   
				String displayChannelName = "";
				if(!lastCharacterValue.contains(lastCharacter.trim())) {
					lastCharacterValue.add(lastCharacter.trim());

					if(!hMapDealerChannelMap.isEmpty()){					  
						displayChannelName = hMapDealerChannelMap.get(lastCharacter);
					}				

					if(strBufferChannels.length()==0){
						strBufferChannels.append(displayChannelName);
					}else{
						strBufferChannels.append(", " + displayChannelName );
					}
				}

			}
			strChannels = strBufferChannels.toString();
			log.info("getChannelsWithoutObjectives :::::"+strChannels);

		} catch (SQLException e) {
			try {
				con.rollback();
			} catch (Exception e2) {
				log.exiting( "Error rollback connection in getDealerDescription: ", e.getMessage());
			}
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "getDealerDescription").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return strChannels;

	}
	// get product country
	public static String getProductCountry(String region) {
		final String  METHOD_NAME = "getProductCountry";
		log.entering(CLASS_NAME, METHOD_NAME);
		String productCountry = "USA";
		if (region != null && region.trim().equals(Continent.EUROPE.getValue()))
			productCountry = "GBR";

		return productCountry;
	}
	public String getMetricTypeDescr(String selectedMetricType) {
		final String  METHOD_NAME = "getMetricTypeDescr";
		log.entering(CLASS_NAME, METHOD_NAME);
		String selectedMetricTypeDescr = "";
		if (selectedMetricType.equals(Constant.BDN_V)) {
			selectedMetricTypeDescr = Constant.UNITS;
		} else if (selectedMetricType.equals(Constant.BDNP_V)) {
			selectedMetricTypeDescr = Constant.VALUES;
		}

		return selectedMetricTypeDescr;
	}
	public List<TrafficLightMetricTypes> getMetricTypes() {

		final String METHOD_NAME = "getMetricTypes";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<TrafficLightMetricTypes> metricTypes = new ArrayList<TrafficLightMetricTypes>();
		TrafficLightMetricTypes record = new TrafficLightMetricTypes();

		record = new TrafficLightMetricTypes();
		record.setLabel(Constant.VALUES);
		record.setValue(Constant.BDNP_V);
		metricTypes.add(record);

		record = new TrafficLightMetricTypes();
		record.setLabel(Constant.UNITS);
		record.setValue(Constant.BDN_V);
		metricTypes.add(record);

		return metricTypes;
	}
	public List<TrafficLightCurrencyTypes> getCurrencyTypes(
			String selectedMarket) {
		final String METHOD_NAME = "getCurrencyTypes";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<TrafficLightCurrencyTypes> currencyTypes = new ArrayList<TrafficLightCurrencyTypes>();
		TrafficLightCurrencyTypes record = new TrafficLightCurrencyTypes();
		record = new TrafficLightCurrencyTypes();
		record.setLabel("USD");
		record.setValue("1");
		currencyTypes.add(record);
		record = new TrafficLightCurrencyTypes();
		record.setLabel("EURO");
		record.setValue("2");
		currencyTypes.add(record);
		// We will uncomment below code for release 2 launch -- BREDDY7
		record = new TrafficLightCurrencyTypes();
		record.setLabel("BER");
		record.setValue("4");
		currencyTypes.add(record);

		if (!selectedMarket.equals(Constant.OTH)) {
			record = new TrafficLightCurrencyTypes();
			record.setLabel("LC");
			record.setValue("3");
			currencyTypes.add(record);
		}

		return currencyTypes;		 
	}
	public String getDealerCodeForDealerGroup(String dealerGroup, String market) {
		final String METHOD_NAME = "getDealerCodeForDealerGroup";
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String dealerCodes = "";
		try {
			con = getConnectionFromFactory();
			StringBuffer sqlString = new StringBuffer();
			sqlString.append("  SELECT  MKT_CUSTOMER_ID_C ");
			sqlString.append(" FROM " + SqlUtils.getDbInstanceView() + ".SGFDM04 ");
			sqlString.append("	WHERE COUNTRY_ISO3_C = ? ");
			sqlString.append("	AND  GFDM04_DLR_GRP_C = ? ");
			log.logp(Level.INFO,METHOD_NAME,"Input Param --",market + ", " + dealerGroup);
			log.logp(Level.INFO,METHOD_NAME,"Query --",sqlString.toString());
			pstmt = con.prepareStatement(sqlString.toString());
			pstmt.setString(1, market);
			pstmt.setString(2, dealerGroup);
			StringBuffer dealerDescription = new StringBuffer();
			rs = pstmt.executeQuery();
			while (rs.next()) {
				dealerDescription.append(rs.getString("MKT_CUSTOMER_ID_C").trim() + ",");
			}
			if (dealerDescription.length() > 0) {
				dealerCodes = dealerDescription.substring(0, dealerDescription.length()-1);
			}
		} catch (SQLException e) {
			try {
				con.rollback();
			} catch (Exception e2) {
				log.exiting( "Error rollback connection in getDealerDescription: ", e.getMessage());
			}
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "getDealerDescription").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return dealerCodes;
	}
	public List<AllMarkets> getAllDlrChannels(String selectedMarket) {
		final String  METHOD_NAME = "getAllDlrChannels";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<AllMarkets>  allDlrChannels = new ArrayList<AllMarkets>();
		// Add the total Dealer channels
		AllMarkets record = new AllMarkets();
		record.setLabel(Constant.DEALER_CHANNEL);
		record.setValue("T");
		allDlrChannels.add(record);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		StringBuilder sql = new StringBuilder();
		sql.append(" SELECT DISTINCT MRKT_MARKET_C channel ");
		sql.append(" FROM "  + SqlUtils.getDbInstanceView() +  ".SGFDG50 ");
		sql.append(" WHERE GFDG50_DSPLY_CTRY_C = ? ");
		/*if(selectedMarket.equalsIgnoreCase("RUS") || selectedMarket.equalsIgnoreCase("TUR"))
		{
			sql.append("   AND Substr(mrkt_market_c, 5, 1) IN ('B')  ");
		}
		else
		{
			if(selectedMarket.equalsIgnoreCase("ROM"))
			{
				sql.append("   AND MRKT_MARKET_C in ('EROM1')  ");
			}
			else
			{*/
		sql.append("   AND Substr(mrkt_market_c, 5, 1) IN ( '1','2' )  ");
		//}
		//	}
		sql.append(" ORDER BY 1 ");
		log.logp(Level.INFO,METHOD_NAME,"Query To Fetch All Delaer channels =",sql.toString());		
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			rs = pstmt.executeQuery();

			while (rs.next()) {				 
				String tempChannel = rs.getString("channel").trim();				 
				String lastCharacter = tempChannel.substring(tempChannel.length()-1, tempChannel.length());				   
				String displayChannelName = "";				 
				if(!hMapDealerChannelMap.isEmpty()){					  
					displayChannelName = hMapDealerChannelMap.get(lastCharacter);
				}
				record = new AllMarkets();
				/* if(selectedMarket.equalsIgnoreCase("RUS") || selectedMarket.equalsIgnoreCase("TUR") || selectedMarket.equalsIgnoreCase("ROM"))
				 {
					 record.setLabel("Dealer");
				 }
				 else
				 {*/
				record.setLabel(displayChannelName);
				// }
				record.setValue(rs.getString("channel").trim());
				allDlrChannels.add(record);
			}
			record = new AllMarkets();
			record.setLabel(Constant.DEALER_GROUP);
			record.setValue(Constant.DEALER_GROUP);
			allDlrChannels.add(record);

		} catch (Exception e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "getAllDlrChannels").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs , pstmt, con );
		}
		return allDlrChannels;
	}
	public List<AllMarkets> getAllDlrGroups(String selectedMarket) {
		final String METHOD_NAME = "getAllDlrGroups";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<AllMarkets>  allDlrGroups = new ArrayList<AllMarkets>();
		// Add the total Dealer Groups
		AllMarkets record = new AllMarkets();
		record.setLabel(Constant.DEALER_GROUP);
		record.setValue("T");
		allDlrGroups.add(record);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		StringBuilder sql = new StringBuilder();
		sql.append(" SELECT	 distinct GFDM04_DLR_GRP_C dealerGroup ");
		sql.append(" FROM "  + SqlUtils.getDbInstanceView() +  ".SGFDM04 ");
		sql.append(" WHERE	COUNTRY_ISO3_C= ? ");
		log.logp(Level.INFO,METHOD_NAME,"Query To Fetch All Dealer Groups =",sql.toString());		
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			rs = pstmt.executeQuery();

			// get the result and construct query object
			while (rs.next()) {
				record = new AllMarkets();
				record.setLabel(rs.getString("dealerGroup").trim());
				record.setValue(rs.getString("dealerGroup").trim());
				allDlrGroups.add(record);
			}
		} catch (Exception e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "getAllDlrGroups").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs , pstmt, con );
		}
		return allDlrGroups;
	}
	public List<AllMarkets>  getDealerRegionPerChannel(String market, String channel) {

		final String  METHOD_NAME = "getDealerRegionPerChannel";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<AllMarkets>  allDlrRegionsPerMarket = new ArrayList<AllMarkets>();
		// Add the total markets
		AllMarkets record =  new AllMarkets();
		record.setLabel(Constant.DEALER_REGION);
		record.setValue("T");
		allDlrRegionsPerMarket.add(record);
		if (!market.equals("") && !channel.equals("")) {
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			Connection con = null;
			StringBuilder sqlString = new StringBuilder();
			sqlString.append(" SELECT DISTINCT m01.OLEV1_SA_LEVEL1_C region ");
			sqlString.append(" FROM " + SqlUtils.getDbInstanceView() + ".SGFDM01 m01, ");
			sqlString.append(  SqlUtils.getDbInstanceView() + ".SGFDM14 m14 ");
			sqlString.append(" WHERE m01.COUNTRY_ISO3_C = m14.COUNTRY_ISO3_C ");
			sqlString.append(" AND m01.MKT_CUSTOMER_ID_C = m14.MKT_CUSTOMER_ID_C ");
			sqlString.append(" AND CHAR2HEXINT(TRIM(m01.MKT_CUSTOMER_ID_C)) <> '00000000000000' ");
			sqlString.append(" AND m01.COUNTRY_ISO3_1_C = ? ");
			sqlString.append(" AND m01.MRKT_MARKET_C = ? ");
			sqlString.append(" AND m01.MKT_CUST_TYPE_C <>  '9' ");
			sqlString.append(" AND m01.MNMKT_MAIN_MKT_C =  'D' ");
			sqlString.append(" ORDER BY m01.OLEV1_SA_LEVEL1_C ; ");
			log.logp(Level.INFO,METHOD_NAME,"Input Param To Fetch Dealer Region Per Market: market=",market
					+ "Channel, " + channel);
			log.logp(Level.INFO,METHOD_NAME,"Query To Fetch Dealer Region Per Market =",sqlString.toString());
			try {
				con = getConnectionFromFactory();
				pstmt = con.prepareStatement(sqlString.toString());
				pstmt.setString(1, market);
				pstmt.setString(2, channel);
				rs = pstmt.executeQuery();

				// get the result and construct query object
				while (rs.next()) {
					record =  new AllMarkets();
					record.setLabel(rs.getString("region").trim());
					record.setValue(rs.getString("region").trim());
					allDlrRegionsPerMarket.add(record);
				}

			} catch (Exception e) {
				throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
						CLASS_NAME, "getDealerRegionPerChannel").build(), "",
						e.getLocalizedMessage());
			} finally {
				closeConnectionResultSet(rs , pstmt, con );
			}
		}

		return allDlrRegionsPerMarket;
	}

	//10108783	IBIS EU DSPR - TLR Label with Data Date
	public Map<String, Date> fetchPostingDates() {
		Map<String, Date> records = new LinkedHashMap<String, Date>();

		StringBuffer sqlString = new StringBuffer();
		sqlString.append("SELECT ");
		sqlString.append("GFDG32_MARKET_C marketCode, ");
		sqlString.append("GFDG32_UPDTRAN_Y updatedTnsDate ");
		sqlString.append(" FROM ");
		sqlString.append(SqlUtils.getDbInstanceView() + ".SGFDG32");
		sqlString.append(" WHERE GFDA07_REGION_C = 'EU' ");
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			// get connection
			con = getConnectionFromFactory();
			con.setAutoCommit(false);
			pstmt = con.prepareStatement(sqlString.toString());
			rs = pstmt.executeQuery();

			while (rs.next()) {
				records.put(rs.getString("marketCode"), rs.getDate("updatedTnsDate"));
			}

		} catch (SQLException e) {
			try {
				con.rollback();
			} catch (Exception e2) {
			}

			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "fetchPostingDates").build(), "",
					e.getLocalizedMessage());
		} finally {
			try {
				// clean up
				if (pstmt != null)
					pstmt.close();
				con.setAutoCommit(true);
				if (con != null)
					con.close();
			} catch (Exception e) {
			}
		}

		return records;
	}
	public List<AllMarkets>  getDealerZonePerChannel(String market, String channel, String region) {

		final String METHOD_NAME = "getDealerZonePerChannel";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<AllMarkets>  allDealerZonePerMarket = new ArrayList<AllMarkets>();
		// Add the total markets
		AllMarkets record =  new AllMarkets();
		record.setLabel(Constant.DEALER_ZONE);
		record.setValue("T");
		allDealerZonePerMarket.add(record);
		if (!market.equals("") && !channel.equals("") && !region.equals("")) {
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			Connection con = null;
			StringBuilder sqlString = new StringBuilder();
			sqlString.append(" SELECT	DISTINCT m01.OLEV3_SA_LEVEL3_C zon ");
			sqlString.append(" FROM " + SqlUtils.getDbInstanceView() + ".SGFDM01 m01, ");
			sqlString.append(  SqlUtils.getDbInstanceView() + ".SGFDM14 m14 ");
			sqlString.append(" WHERE m01.COUNTRY_ISO3_C = m14.COUNTRY_ISO3_C");
			sqlString.append(" AND m01.MKT_CUSTOMER_ID_C = m14.MKT_CUSTOMER_ID_C ");
			sqlString.append(" AND CHAR2HEXINT(TRIM(m01.MKT_CUSTOMER_ID_C)) <> '00000000000000' ");
			sqlString.append(" AND m01.COUNTRY_ISO3_1_C = ? ");
			sqlString.append(" AND m01.MRKT_MARKET_C = ? ");
			sqlString.append(" AND m01.OLEV1_SA_LEVEL1_C = ? ");
			sqlString.append(" AND m01.MKT_CUST_TYPE_C <>  '9' ");
			sqlString.append(" AND m01.MNMKT_MAIN_MKT_C =  'D' ");
			sqlString.append(" ORDER BY m01.OLEV3_SA_LEVEL3_C ");
			log.logp(Level.INFO,METHOD_NAME,"Input Param To Fetch Dealer Zone Per Channel: market= ",market
					+ " channel=, " + channel + "region=, " + region);
			log.logp(Level.INFO,METHOD_NAME,"Query To Fetch Dealer Zone Per Channel =",sqlString.toString());
			try {
				con = getConnectionFromFactory();
				pstmt = con.prepareStatement(sqlString.toString());
				pstmt.setString(1, market);
				pstmt.setString(2, channel);
				pstmt.setString(3, region);
				rs = pstmt.executeQuery();
				// get the result and construct query object
				while (rs.next()) {
					record =  new AllMarkets();
					record.setLabel(rs.getString("zon").trim());
					record.setValue(rs.getString("zon").trim());
					allDealerZonePerMarket.add(record);
				}

			} catch (Exception e) {
				throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
						CLASS_NAME, "getDealerZonePerChannel").build(), "",
						e.getLocalizedMessage());
			} finally {
				closeConnectionResultSet(rs , pstmt, con );
			}
		}

		return allDealerZonePerMarket;
	}
	public List<AllMarkets>  getDealersPerChannel(String market, String channel, String region, String Zone) {

		final String METHOD_NAME = "getDealersPerChannel";
		log.entering(CLASS_NAME, METHOD_NAME);
		List<AllMarkets>  allDealersPerMarket = new ArrayList<AllMarkets>();
		// Add the total markets
		AllMarkets record =  new AllMarkets();
		record.setLabel(Constant.ALL_DEALER);
		record.setValue("T");
		allDealersPerMarket.add(record);
		if(!market.equals("") && !channel.equals("") && !region.equals("") && !Zone.equals("")) {
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			Connection con = null;
			StringBuilder sqlString = new StringBuilder();
			sqlString.append("  SELECT	DISTINCT TRIM(m01.MKT_CUSTOMER_ID_C) dealerID, ");
			sqlString.append("	(TRIM(m01.MKT_CUSTOMER_ID_C) || '-' || TRIM(m01.SSA_TRADE_N)) dealerDescription ");
			sqlString.append("  FROM " + SqlUtils.getDbInstanceView() + ".SGFDM01 m01, ");
			sqlString.append(	SqlUtils.getDbInstanceView() + ".SGFDM14 m14 ");
			sqlString.append("  WHERE m01.COUNTRY_ISO3_C = m14.COUNTRY_ISO3_C ");
			sqlString.append(" 	AND m01.MKT_CUSTOMER_ID_C = m14.MKT_CUSTOMER_ID_C  ");
			sqlString.append("	AND CHAR2HEXINT(TRIM(m01.MKT_CUSTOMER_ID_C)) <> '00000000000000' "); // Remove bad dealer codes
			sqlString.append(" 	AND m01.COUNTRY_ISO3_1_C = ?  ");
			sqlString.append(" 	AND m01.MRKT_MARKET_C = ? ");
			sqlString.append(" 	AND m01.OLEV1_SA_LEVEL1_C = ? ");
			sqlString.append(" 	AND m01.OLEV3_SA_LEVEL3_C = ? ");
			sqlString.append("  AND m01.MKT_CUST_TYPE_C <>  '9' ");
			sqlString.append("  AND m01.MNMKT_MAIN_MKT_C =  'D' ");
			sqlString.append("  ORDER BY m01.MKT_CUSTOMER_ID_C ");
			log.logp(Level.INFO,METHOD_NAME,"Input Param To Fetch Dealer Per Market: market= ",market + " channel= "
					+ channel + " region= " + region + "Zone= " + Zone);
			log.logp(Level.INFO,METHOD_NAME,"Query To Fetch Dealer Per Channel =", sqlString.toString());
			try {
				con = getConnectionFromFactory();
				pstmt = con.prepareStatement(sqlString.toString());
				pstmt.setString(1, market);
				pstmt.setString(2, channel);
				pstmt.setString(3, region);
				pstmt.setString(4, Zone);
				rs = pstmt.executeQuery();
				// get the result and construct query object
				while (rs.next()) {
					record =  new AllMarkets();
					record.setLabel(rs.getString("dealerDescription").trim());
					record.setValue(rs.getString("dealerID").trim());
					allDealersPerMarket.add(record);
				}

			} catch (Exception e) {
				throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
						CLASS_NAME, "getDealersPerChannel").build(), "",
						e.getLocalizedMessage());
			} finally {
				closeConnectionResultSet(rs , pstmt, con );
			}
		}

		return allDealersPerMarket;
	}
	public ProductHierarchyRecord getMLITreeNode( ) {	
		final String  METHOD_NAME = "getMLITreeNode";
		log.entering(CLASS_NAME, METHOD_NAME);
		final String PATTERN1 = "#,##0.00;(#,##0.00)";
		//final String PATTERN2 = "$#,##0.00;-$#,##0.00";
		DecimalFormat df = (DecimalFormat) DecimalFormat.getInstance();
		df.applyPattern(PATTERN1);
		ProductHierarchyRecord productRoot = new ProductHierarchyRecord();
		// PCT
		List<ProductHierarchyRecord> pctList = ProductHierarchyModel
				.getAllPCTRecordsForTrafficLight();
		productRoot.setChildren(pctList); // set root children to pct
		Map<String, List<ProductHierarchyRecord>> allProducts = ProductHierarchyModel.getAllProducts();
		// CG
		for (ProductHierarchyRecord pct : pctList) {
			pct.setParent(productRoot); // set pct parent to root
			List<ProductHierarchyRecord> cgList = ProductHierarchyModel.getChildProductHierarchyRecords(allProducts, pct);
			pct.setChildren(cgList); // set pct children to cg

			if (cgList != null) {
				for (ProductHierarchyRecord cg : cgList) {
					cg.setParent(pct); // set cg parent to pct
					// MPL
					List<ProductHierarchyRecord> mplList = ProductHierarchyModel.getChildProductHierarchyRecords(allProducts, cg);
					cg.setChildren(mplList); // set cg children to mpl
					if (mplList != null) {
						for (ProductHierarchyRecord mpl : mplList) {
							mpl.setParent(cg); // set mpl parent to cg
							// MLI
							List<ProductHierarchyRecord> mliList = ProductHierarchyModel.getChildProductHierarchyRecords(allProducts, mpl);
							mpl.setChildren(mliList); // set mpl children to mli
							if (mliList != null)
								for (ProductHierarchyRecord mli : mliList) {
									mli.setParent(mpl);
									mli.setChildren(null);
								}
						}
					}
				}
			}
		}
		return productRoot;
	}
	public String getTransactionDate(boolean yearStrat ){	
		final String  METHOD_NAME = "getTransactionDate";
		log.entering(CLASS_NAME, METHOD_NAME);
		Connection con = null;	
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String transDate="";

		StringBuffer sqlString = new StringBuffer();
		if(yearStrat){
			sqlString.append(" SELECT	 max(GFDG30_MNTHYR_Y)   transDate"); 
		}else{
			sqlString.append(" SELECT	 max(GFDG30_MNTHYR_Y)- 1 transDate"); 
		}

		sqlString.append(" FROM " + SqlUtils.getDbInstanceView() + ".SGFDG30 "); 
		sqlString.append(" WHERE GFDG30_METRIC_C = 'CPV' "); 
		sqlString.append(" AND GFDA07_REGION_C = ? ");
		String region = IbisSession.getUserProfile().getCurrentRegion();	
		log.logp(Level.INFO,METHOD_NAME,"Input Param To Fetch Transaction Date region=", region);
		log.logp(Level.INFO,METHOD_NAME,"Query To Get Transaction Date =",sqlString.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sqlString.toString());	

			pstmt.setString(1, region); 
			rs = pstmt.executeQuery();

			while(rs.next()){
				transDate = rs.getString("transDate")	;
			}
		}
		catch (SQLException e){
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "getTransactionDate").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs , pstmt, con );
		}

		if(transDate.substring(4,6).equals("00")){
			int preYear = Integer.parseInt(transDate.substring(0,4)) - 1;
			transDate = new Integer(preYear).toString() + "12";
		}

		return transDate;

	}
	public Map<String, List<DailySalesPerformanceEURecord>> getFilteredTLRData(String selectedMarket, String selectedMonth,
			ProductHierarchy productType, String selectedDlrChannel, String selectedDlrRegion, 
			String selectedDlrZone, String selectedDealers, String selectedCurrencyType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredTLRData";
		log.entering(CLASS_NAME, METHOD_NAME);

		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();
		//	hMapCountryPostedDateWorkingDaysNWorkingDays = retrivePostedDateDaysNWorkingDays(selectedMarket);

		int objectiveAtProductLevel = 0;

		boolean displayObjectives = true;
		boolean displayWarranty = true;

		if(!"".equals(selectedMarket) && !"".equals(selectedDlrChannel)){

			if (hmapObjectiveLevels.containsKey(selectedMarket)){

				HashMap<String,Integer> hmapTemp = new HashMap<String,Integer>();

				hmapTemp = hmapObjectiveLevels.get(selectedMarket);

				if(hmapTemp.containsKey(selectedDlrChannel)){			

					objectiveAtProductLevel = hmapTemp.get(selectedDlrChannel);
				}else{

					objectiveAtProductLevel = hmapTemp.get("OTHER");
				}


			}

		}

		if (objectiveAtProductLevel > productType.getValue() ){

			displayObjectives = false;
		}
		log.info("Objectives Level Defined " +objectiveAtProductLevel+"PRODUCT TYPE VALUE ::::::::"+productType.getValue()+"Objectives Flag "+ displayObjectives);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R,  MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}


		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(" GFDG50_CG_C, ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(" GFDG50_CG_C, GFDG50_MPL_C, ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(" GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C, ");
		}

		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		// Proj. CPV vs PY is a derived value projectedVsPY
		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		/*sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, ");
	sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");
			sql.append("sum(GFDG50_VW_PROJ_A)  warranty, ");
			sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");
			sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) as grossCPVProjected, ");
			sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, ");
			sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");
			sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, ");
	//sql.append("sum(GFDG50_BDN_TCYTCMD_T) grossCPVProjYTD, ");
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS TPYTPYCMD, ");*/
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND   Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//				sqlString.append("    ) )       ");
		}
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		//sql.append("group by GFDG50_PCT_C ");
		/*if (productType.equals(ProductHierarchy.CG)) {
				sql.append(", GFDG50_CG_C ");
			} else if (productType.equals(ProductHierarchy.MPL)) {
				sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
			} else if (productType.equals(ProductHierarchy.MLI)) {
				sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
			}*/
		if(productType.equals(ProductHierarchy.PCT)){
			sql.append("group by GFDG50_PCT_C ");
		}else if (productType.equals(ProductHierarchy.CG)) {
			sql.append(" and  GFDG50_CG_C <>'' group by GFDG50_PCT_C, GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append("and GFDG50_MPL_C <>''  group by GFDG50_PCT_C, GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append("and GFDG50_MLI_C <>'' group by GFDG50_PCT_C, GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		sql.append(", MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");
		sql.append("order by GFDG50_PCT_C ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(", GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		log.logp(Level.INFO,METHOD_NAME, productType + ":= ", sql.toString());
		log.info("main query in TLR+++++"+ sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			boolean isDivideBy1000 = false;
			if(selectedDealers.equals(Constant.T)){
				isDivideBy1000 = true;
			}
			if(!selectedDlrRegion.equals(Constant.T)) {
				displayObjectives = false;
				displayWarranty = false;
			}
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, productType, displayObjectives,
						hMapCountryPostedDateWorkingDaysNWorkingDays, isDivideBy1000, selectedCurrencyType, "", selectedMarket, previousYearRate, warrantyForecast, displayWarranty);
			}
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return dataMap;
	}
	public Map<String, List<DailySalesPerformanceEURecord>> getTLRLevelData(String selectedMarket, String selectedMonth, boolean strObjDisplayFlag,
			ProductHierarchy productType, String selectedDlrChannel,String selectedCurrencyType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getTLRLevelData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();		
		log.info("hMapCountryPostedDateWorkingDaysNWorkingDays--"+hMapCountryPostedDateWorkingDaysNWorkingDays);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT GFDG50_PCT_C ");
		//sql.append(",MONEX_LOCAL_RATE_R, MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R budget, ");
		//sql.append(",MONEX_LOCAL_RATE_R, MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}


		if (productType.equals(ProductHierarchy.CG)) {
			sql.append("GFDG50_CG_C, ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C, ");
		}
		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		// Proj. CPV vs PY is a derived value projectedVsPY

		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}

		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, "); // last 5 days

		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt "); // last 20 days

		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		if(productType.equals(ProductHierarchy.PCT)){
			sql.append("group by GFDG50_PCT_C ");
		}else if (productType.equals(ProductHierarchy.CG)) {
			sql.append(" and  GFDG50_CG_C <>'' group by GFDG50_PCT_C, GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append("and GFDG50_MPL_C <>'' group by GFDG50_PCT_C, GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append("and GFDG50_MLI_C <>'' group by GFDG50_PCT_C, GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}

		sql.append(", MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");

		sql.append("order by GFDG50_PCT_C ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(", GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		log.logp(Level.INFO, METHOD_NAME, productType + ":= ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, productType, strObjDisplayFlag, hMapCountryPostedDateWorkingDaysNWorkingDays,
						true, selectedCurrencyType, "", selectedMarket, previousYearRate, warrantyForecast, true);
			}
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return dataMap;
	}
	public DailySalesPerformanceEURecord getFilteredGrandTotalData(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getFilteredGrandTotalData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String, List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String, List<Integer>>();
		//	hMapCountryPostedDateWorkingDaysNWorkingDays = retrivePostedDateDaysNWorkingDays(selectedMarket);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord grandTotalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		boolean displayWarranty = true;
		//sql.append("SELECT 'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT  'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}

		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.		
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12		
		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY		
		// Proj. CPV vs PY is a derived value projectedVsPY		
		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		/*sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, ");
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");
		sql.append("sum(GFDG50_VW_PROJ_A)  warranty, ");
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) as grossCPVProjected, ");
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, ");
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");
		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_T) grossCPVProjYTD, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS TPYTPYCMD, ");*/
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");

		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();
			boolean isDivideBy1000 = false;
			if (selectedDealers.equals(Constant.T)) {
				isDivideBy1000 = true;
			}
			boolean isSelectedDealerRegion = true;
			if(!selectedDlrRegion.equalsIgnoreCase(Constant.T)) {
				isSelectedDealerRegion = false;
				displayWarranty = false;
			}
			while (rs.next()) {
				processResultSet(rs, dataMap, null, isSelectedDealerRegion, hMapCountryPostedDateWorkingDaysNWorkingDays, 
						isDivideBy1000, selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, displayWarranty);
			}
			if(!dataMap.isEmpty())
				grandTotalRecord = dataMap.get(Constant.GRAND_TOTAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return grandTotalRecord;
	}

	public DailySalesPerformanceEURecord getFilteredMechanicalData(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getFilteredMechanicalData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String, List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String, List<Integer>>();
		//	hMapCountryPostedDateWorkingDaysNWorkingDays = retrivePostedDateDaysNWorkingDays(selectedMarket);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoMechanicalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");


		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}



		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.		
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12		
		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY		
		// Proj. CPV vs PY is a derived value projectedVsPY		
		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");

		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("and GFDG50_PCT_C in ('L', 'P', 'M', 'O')");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");

		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();
			boolean isDivideBy1000 = false;
			boolean isSelectedDealerRegion = true;
			boolean displayWarranty = true;
			if(!selectedDlrRegion.equalsIgnoreCase(Constant.T)) {
				isSelectedDealerRegion = false;
				displayWarranty = false;
			}
			if (selectedDealers.equals(Constant.T)) {
				isDivideBy1000 = true;
			}
			while (rs.next()) {
				processResultSet(rs, dataMap, null, isSelectedDealerRegion, hMapCountryPostedDateWorkingDaysNWorkingDays, 
						isDivideBy1000, selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, displayWarranty);
			}
			if(!dataMap.isEmpty())
				memoMechanicalRecord = dataMap.get(Constant.MEMO_MECHANICAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoMechanicalRecord;
	}

	public DailySalesPerformanceEURecord getFilteredMemoTiresData(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getFilteredMemoTiresData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String, List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String, List<Integer>>();
		//	hMapCountryPostedDateWorkingDaysNWorkingDays = retrivePostedDateDaysNWorkingDays(selectedMarket);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoTiresRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//	sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}


		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.		
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12		
		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY		
		// Proj. CPV vs PY is a derived value projectedVsPY		
		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");


		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append(" AND GFDG50_PCT_C in ('L','B') ");
		sql.append(" AND GFDG50_MLI_C in ('0098' , '0120') ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");

		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();
			boolean isDivideBy1000 = false;
			boolean displayWarranty = true;
			if (selectedDealers.equals(Constant.T)) {
				isDivideBy1000 = true;
			}
			if(!selectedDlrRegion.equals(Constant.T)) {
				displayWarranty = false;
			}
			while (rs.next()) {
				processResultSet(rs, dataMap, null, false, hMapCountryPostedDateWorkingDaysNWorkingDays, 
						isDivideBy1000, selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, displayWarranty);
			}
			if(!dataMap.isEmpty())
				memoTiresRecord = dataMap.get(Constant.MEMO_TIRES).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoTiresRecord;
	}

	public DailySalesPerformanceEURecord getFilteredGrandTotalExclCBData(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getFilteredGrandTotalExclCBData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String, List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String, List<Integer>>();
		//	hMapCountryPostedDateWorkingDaysNWorkingDays = retrivePostedDateDaysNWorkingDays(selectedMarket);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord getGrandTotalExclCBRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'GRAND TOTAL excl. CB'  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}

		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.		
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12		
		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY		
		// Proj. CPV vs PY is a derived value projectedVsPY		
		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND    Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("  and GFDG50_PCT_C <> 'B' ");
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");

		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();
			boolean isDivideBy1000 = false;
			if (selectedDealers.equals(Constant.T)) {
				isDivideBy1000 = true;
			}
			boolean isSelectedDealerRegion = true;
			boolean displayWarranty = true;
			if(!selectedDlrRegion.equalsIgnoreCase(Constant.T)) {
				isSelectedDealerRegion = false;
				displayWarranty = false;
			}
			while (rs.next()) {
				processResultSet(rs, dataMap, null, isSelectedDealerRegion, hMapCountryPostedDateWorkingDaysNWorkingDays, 
						isDivideBy1000, selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, displayWarranty);
			}
			if(!dataMap.isEmpty())
				getGrandTotalExclCBRecord = dataMap.get(Constant.GRAND_TOTAL_EX_CB).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return getGrandTotalExclCBRecord;
	}


	public DailySalesPerformanceEURecord getGrandTotalData(String selectedMarket, String selectedMonth,
			boolean isDivideBy1000,String selectedDlrChannel, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getGrandTotalData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();	
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord grandTotalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT  'Grand Total' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'Grand Total'  GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}

		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		// Proj. CPV vs PY is a derived value projectedVsPY

		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		/*sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, ");
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");
		sql.append("sum(GFDG50_VW_PROJ_A)  warranty, ");
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) as grossCPVProjected, ");
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, ");

	sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_T) grossCPVProjYTD, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS TPYTPYCMD, ");*/
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());
		

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, null, true, hMapCountryPostedDateWorkingDaysNWorkingDays, isDivideBy1000,
						selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, true);
			}
			if(!dataMap.isEmpty())
				grandTotalRecord = dataMap.get(Constant.GRAND_TOTAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return grandTotalRecord;
	}


	public DailySalesPerformanceEURecord getMechanicalTotalData(String selectedMarket, String selectedMonth,String selectedDlrChannel,
			boolean isDivideBy1000, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getMechanicalTotalData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();	
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord getMechanicalTotalData = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}
		//sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		else {
			sql.append("SELECT 'Memo: MECHANICAL' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}
		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("and GFDG50_PCT_C in ('L', 'P', 'M', 'O')");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, null, true, hMapCountryPostedDateWorkingDaysNWorkingDays, isDivideBy1000,
						selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, true);
			}
			if(!dataMap.isEmpty())
				getMechanicalTotalData = dataMap.get(Constant.MEMO_MECHANICAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return getMechanicalTotalData;
	}

	public DailySalesPerformanceEURecord getMemoTiresData(String selectedMarket, String selectedMonth,String selectedDlrChannel,
			boolean isDivideBy1000, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getMemoTiresData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord getMemoTiresData = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget, ");



		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'Memo: TIRES' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}

		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND   Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append("and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append(" AND GFDG50_PCT_C in ('L','B') ");
		sql.append("AND  GFDG50_MLI_C in ('0098' , '0120') ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, null, false, hMapCountryPostedDateWorkingDaysNWorkingDays, isDivideBy1000,
						selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, true);
			}
			if(!dataMap.isEmpty())
				getMemoTiresData = dataMap.get(Constant.MEMO_TIRES).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return getMemoTiresData;
	}

	public DailySalesPerformanceEURecord getGrandTotalExclCBData(String selectedMarket, String selectedMonth,String selectedDlrChannel,
			boolean isDivideBy1000, String selectedCurrencyType, String parentType, Map<String, DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast) {
		final String METHOD_NAME = "getGrandTotalExclCBData";
		log.entering(CLASS_NAME, METHOD_NAME);
		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();	
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord getGrandTotalExclCBData = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		//sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R, ");
		//sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");

		if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
			sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_EURO_RATE_R budget , ");
		}

		else {
			sql.append("SELECT 'GRAND TOTAL excl. CB' GFDG50_PCT_C, MONEX_LOCAL_RATE_R,  MONEX_EURO_RATE_R, MONEX_BUDG_RATE_R/MONEX_LOCAL_RATE_R budget , ");
		}


		sql.append("sum(GFDG50_BDN_MTD_A ) mtdGrossRev, "); // Mtd Gross Revenue
		sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, "); //Proj. Gross Revenue 
		sql.append("sum(GFDG50_VWG_PROJ_A)  warranty, "); // Warranty
		sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, "); // Pricing
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_A) grossCPVProjected, ");	//Gross CPV Proj.
		sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, "); //Gross CPV Obj.
		sql.append("sum(GFDG50_PROJ_VS_OBJ_A)  projectedVsObjective, "); // Proj. vs Obj.  has to be commented later 10/12

		sql.append("sum(GFDG50_CPV_PRIOR_YR_A) grossCPVPY, "); // Gross CPV PY

		if(selectedCurrencyType.equals("2") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_EUR_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_EUR_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("3") ) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_LOCAL_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_LOCAL_A) AS projYtdVsPYTD, ");
		}
		else if(selectedCurrencyType.equals("4")) {
			sql.append("sum(GFDG50_BDN_TCYTCMD_BER_A) AS GrossCPVProjYTD,");  

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_BER_A) AS projYtdVsPYTD, ");
		}
		else {
			sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS GrossCPVProjYTD,");  // Gross CPV Proj. YTD

			sql.append("sum(GFDG50_BDN_TPYTPYCMD_A) AS projYtdVsPYTD, "); // Proj. YTD vs PYTD tpytpycmd TPYTPYCMD
		}
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_A) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_A) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("   AND Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append(" and GFDG50_PCT_C <> 'B' ");
		sql.append(" and GFDG50_OBJ_PROD_MKT_LVL_C is not null ");
		sql.append("and MONEX_LOCAL_RATE_R <> 0 ");
		sql.append("group by MONEX_LOCAL_RATE_R ");
		sql.append(",MONEX_EURO_RATE_R ");
		sql.append(",budget ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processResultSet(rs, dataMap, null, true, hMapCountryPostedDateWorkingDaysNWorkingDays, isDivideBy1000,
						selectedCurrencyType, parentType, selectedMarket, previousYearRate, warrantyForecast, true);
			}
			if(!dataMap.isEmpty())
				getGrandTotalExclCBData = dataMap.get(Constant.GRAND_TOTAL_EX_CB).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return getGrandTotalExclCBData;
	}

	private void processResultSet(ResultSet rs,	Map<String, List<DailySalesPerformanceEURecord>> dataMap,
			ProductHierarchy productType, boolean objectiveDisplayFlag, HashMap<String,List<Integer>> hMapTemp,
			boolean isDivideBy1000, String selectedCurrencyType, String parentType, String selectedMarket, Map<String, 
			DailySalesPerformanceEURate> previousYearRate, Map<String, String> warrantyForecast, boolean displayWarranty) throws SQLException {
		DailySalesPerformanceEURecord record = new DailySalesPerformanceEURecord();
		DecimalFormat decimalFormatter = new DecimalFormat("#,###.0;(#,###.0)");
		DecimalFormat numberFormatter = new DecimalFormat("#,###;(#,###)");

		// Fetching from Result set
		double mtdGrossRev = rs.getDouble("mtdGrossRev");	
		double projGrossRev = rs.getDouble("projGrossRev");
		double warranty = rs.getDouble("warranty");
		double newPrice = rs.getDouble("newPrice");
		double grossCPVPY = rs.getDouble("grossCPVPY");
		// System.out.println("projectedVsPY ###$#$$#$---"+grossCPVPY);
		double grossCPVProjected = rs.getDouble("grossCPVProjected");
		// System.out.println("grossCPVProjected----"+grossCPVProjected);
		double grossCPVObjective = rs.getDouble("grossCPVObjective");
		//	double projectedVsObjective = rs.getDouble("projectedVsObjective");
		double GrossCPVProjYTD = rs.getDouble("GrossCPVProjYTD");
		// System.out.println("GrossCPVProjYTD----"+GrossCPVProjYTD);
		double projYtdVsPYTD = rs.getDouble("projYtdVsPYTD");
		// System.out.println("projYtdVsPYTD----"+projYtdVsPYTD);
		double avg5 = rs.getDouble("last5daysAmt");// Changes 08/21
		double avg20 = rs.getDouble("last20daysAmt");// Changes 08/21
		// Local currency calculation
		double localRate = rs.getDouble("MONEX_LOCAL_RATE_R");
		// Local currency calculation
		double euroRate = rs.getDouble("MONEX_EURO_RATE_R");
		//	double berRate = rs.getDouble("MONEX_BUDG_RATE_R");
		double berRate = rs.getDouble("budget");
		// Variable for calculation
		double projectedVsObjective = 0;  // derived value 
		double projectedVsPY = 0;
		double ytdVsPY = 0;  // derives value
		double average5 = 0;
		double average20 = 0;
		
		DailySalesPerformanceEURate dsprEuRate = new DailySalesPerformanceEURate();

		if(previousYearRate.get(selectedMarket) != null) {
			dsprEuRate = previousYearRate.get(selectedMarket);
		}
		boolean displayWarrantyFlag = false;
		if(!warrantyForecast.isEmpty()) {
			String warrantyFlag = warrantyForecast.get(rs.getString("GFDG50_PCT_C").trim());
			if(warrantyFlag!= null && warrantyFlag.equalsIgnoreCase("Y")) {
				displayWarrantyFlag = true;
				displayWarrantyForGrandTotal = false;
			}
		}
		
		if((productType != null && !productType.equals(ProductHierarchy.PCT) && displayWarrantyFlag) 
				|| (productType != null && !displayWarranty && displayWarrantyFlag && productType.equals(ProductHierarchy.PCT))
				|| (productType == null && !displayWarranty && !displayWarrantyForGrandTotal)) {
			warranty = 0;
			grossCPVProjected = 0;
			//grossCPVPY = 0;
			GrossCPVProjYTD = 0;
			projYtdVsPYTD = 0;
			
		}
		
		/*// Changes 08/21 */
		if (avg5 != 0) {
			average5 = avg5 / 5;
		}
		if (avg20 != 0) {
			average20 = avg20 / 20;
		}

		if (grossCPVObjective != 0) {
			projectedVsObjective = (grossCPVProjected / grossCPVObjective) * 100;
		}
		else{
			projectedVsObjective=0;
		}
		if(projYtdVsPYTD !=0){
			ytdVsPY = ((GrossCPVProjYTD / projYtdVsPYTD) * 100);//tpytpycmd
			if (ytdVsPY >= 102.5) {
				record.setYtdVsPYImagePath("/resources/images/green.gif");
			} else if (ytdVsPY < 97.5) {
				record.setYtdVsPYImagePath("/resources/images/red.gif");
			} else {
				record.setYtdVsPYImagePath("/resources/images/yellow.gif");
			}
		}else{
			record.setYtdVsPYImagePath("/resources/images/yellow.gif");
		}
		if (grossCPVPY != 0) {
			projectedVsPY =  grossCPVProjected/grossCPVPY * 100;

			if (projectedVsPY >= 102.5){
				record.setImgPath("/resources/images/green.gif");
			} else if (projectedVsPY < 97.5) {
				record.setImgPath("/resources/images/red.gif");
			} else {
				record.setImgPath("/resources/images/yellow.gif");
			}
		} else {
			record.setImgPath("/resources/images/yellow.gif");
		}
		if (average20 != 0) {//Changes 08/21
			if ((average5 / average20) >= 1.025) {//Changes 08/211.076
				record.setArrowPath("/resources/images/over.gif");
			} else if ((average5 / average20) < 0.975) {//Changes 08/21
				record.setArrowPath("/resources/images/under.gif");
			} else {
				record.setArrowPath("/resources/images/right.gif");
			}
		} 
		else {
			record.setArrowPath("/resources/images/right.gif");
		}

		if (selectedCurrencyType.equals("3") && localRate != 0) {
			mtdGrossRev = mtdGrossRev / localRate;		
			projGrossRev = projGrossRev / localRate;
			warranty = warranty / localRate;
			newPrice = newPrice / localRate;
			if(dsprEuRate.getBookKeepingRatePreviousYear() != 0) {
				grossCPVPY = grossCPVPY / dsprEuRate.getBookKeepingRatePreviousYear();
			}
			else {
				grossCPVPY = grossCPVPY / localRate;
			}
			grossCPVProjected = grossCPVProjected / localRate;
			grossCPVObjective = grossCPVObjective / localRate;
			/*GrossCPVProjYTD = GrossCPVProjYTD / localRate; //tcytcmd
			projYtdVsPYTD = projYtdVsPYTD / localRate;*/			
			avg5 = avg5 / localRate;
			avg20 = avg20 / localRate;
		}else if(selectedCurrencyType.equals("2")){
			mtdGrossRev = mtdGrossRev / euroRate;		
			projGrossRev = projGrossRev / euroRate;
			warranty = warranty / euroRate;
			newPrice = newPrice / euroRate;
			if(dsprEuRate.getEuroRatePreviousYear() != 0) {
				grossCPVPY = grossCPVPY / dsprEuRate.getEuroRatePreviousYear();
			}
			else {
				grossCPVPY = grossCPVPY / euroRate;
			}
			grossCPVProjected = grossCPVProjected / euroRate;
			grossCPVObjective = grossCPVObjective / euroRate;
			/*GrossCPVProjYTD = GrossCPVProjYTD / euroRate; //tcytcmd
			projYtdVsPYTD = projYtdVsPYTD / euroRate;*/			
			avg5 = avg5 / euroRate;
			avg20 = avg20 / euroRate;
		}

		/*else if(selectedCurrencyType.equals("4")){
			mtdGrossRev = mtdGrossRev / berRate;		
			projGrossRev = projGrossRev / berRate;
			warranty = warranty / berRate;
			newPrice = newPrice / berRate;
			grossCPVPY = grossCPVPY / berRate;
			grossCPVProjected = grossCPVProjected / berRate;
			grossCPVObjective = grossCPVObjective / berRate;
			GrossCPVProjYTD = GrossCPVProjYTD / berRate; //tcytcmd
			projYtdVsPYTD = projYtdVsPYTD / berRate;			
			avg5 = avg5 / berRate;
			avg20 = avg20 / berRate;
		}*/

		else if(selectedCurrencyType.equals("4")){
			mtdGrossRev = mtdGrossRev * berRate;		
			projGrossRev = projGrossRev * berRate;
			warranty = warranty * berRate;
			newPrice = newPrice * berRate;
			if(dsprEuRate.getEuroRatePreviousYear() != 0) {
				if(selectedMarket.equalsIgnoreCase("EDM") || selectedMarket.equalsIgnoreCase("ROM") || selectedMarket.equalsIgnoreCase("TUR")) {
					grossCPVPY = grossCPVPY *(dsprEuRate.getBudgetRateCurrentYear()/dsprEuRate.getEuroRatePreviousYear());
				}
				else {
					grossCPVPY = grossCPVPY * (dsprEuRate.getBudgetRateCurrentYear()/dsprEuRate.getBookKeepingRatePreviousYear());
				}
			}
			else {
				grossCPVPY = grossCPVPY * berRate;
			}
			grossCPVProjected = grossCPVProjected * berRate;
			grossCPVObjective = grossCPVObjective * berRate;
			/*GrossCPVProjYTD = GrossCPVProjYTD * berRate; //tcytcmd
			projYtdVsPYTD = projYtdVsPYTD * berRate;*/			
			avg5 = avg5 * berRate;
			avg20 = avg20 * berRate;
		}


		// If All Dealers is selected then divide by 1000
		if (isDivideBy1000) {
			mtdGrossRev = mtdGrossRev / 1000;

			projGrossRev = projGrossRev / 1000;
			warranty = warranty / 1000;
			newPrice = newPrice / 1000;
			grossCPVPY = grossCPVPY / 1000;
			grossCPVProjected = grossCPVProjected / 1000;
			grossCPVObjective = grossCPVObjective / 1000;
			GrossCPVProjYTD = GrossCPVProjYTD / 1000;
			projYtdVsPYTD = projYtdVsPYTD / 1000;		
			avg5 = avg5 / 1000;
			avg20 = avg20 / 1000;
		}

		if (mtdGrossRev != 0.0) {
			record.setMtdGrossRev(numberFormatter.format(mtdGrossRev));
		} else {
			record.setMtdGrossRev("");
		}
		if (projGrossRev != 0.0) {
			record.setProjGrossRev(numberFormatter.format(projGrossRev));
		} else {
			record.setProjGrossRev("");
		}
		if (warranty != 0.0) {
			record.setWarranty(numberFormatter.format(warranty));
		} else {
			record.setWarranty("");
		}
		if (newPrice != 0.0) {
			record.setNewPrice(numberFormatter.format(newPrice));
		} else {
			record.setNewPrice("");
		}
		if(grossCPVProjected != 0.0){
			record.setGrossCPVProjected(numberFormatter.format(grossCPVProjected));
		} else {
			record.setGrossCPVProjected("");
		}
		if(objectiveDisplayFlag){
			if(grossCPVObjective != 0.0){
				record.setGrossCPVObjective((numberFormatter.format(grossCPVObjective)));
			} else {
				record.setGrossCPVObjective("");
			}
			if (projectedVsObjective != 0.0) {   // Proj vs Obj
				record.setProjectedVsObjective(decimalFormatter.format(projectedVsObjective) + "%");
			} else {
				record.setProjectedVsObjective("");
			}
		} else {
			record.setGrossCPVObjective("");
			record.setProjectedVsObjective("");
		}

		if (grossCPVPY != 0.0) {
			record.setGrossCPVPY(numberFormatter.format(grossCPVPY));
		} else {
			record.setGrossCPVPY("");
		}
		if (grossCPVProjected < 0 && grossCPVPY < 0) {
			record.setProjectedVsPY("N/A%");
			record.setImgPath("/resources/images/red.gif");
		} else if (projGrossRev != 0 && (grossCPVPY == 0 || grossCPVPY < 0)) {
			record.setProjectedVsPY("N/A%");
			record.setImgPath("/resources/images/green.gif");
		} else if (projectedVsPY > 0.0) {
			record.setProjectedVsPY(decimalFormatter.format(projectedVsPY) + "%");
		} else if (projectedVsPY < 0.0) {
			record.setProjectedVsPY("N/A%");
		} else {
			record.setProjectedVsPY("");
		}

		if (GrossCPVProjYTD != 0.0) {
			record.setYtd(numberFormatter.format(GrossCPVProjYTD));
		} else {
			record.setYtd("");
		}

		if (ytdVsPY != 0.0) {
			record.setYtdVsPY(decimalFormatter.format(ytdVsPY) + "%");
		} else {
			record.setYtdVsPY("");
		}

		String key = "";

		if (productType == null) {
			if(parentType.equals(Constant.GRAND_TOTAL)) {
				key = Constant.GRAND_TOTAL;
			}
			else if(parentType.equals(Constant.MEMO_MECHANICAL)) {
				key = Constant.MEMO_MECHANICAL;
			}
			else if(parentType.equals(Constant.MEMO_TIRES)) {
				key = Constant.MEMO_TIRES;
			}
			else if(parentType.equals(Constant.GRAND_TOTAL_EX_CB)) {
				key = Constant.GRAND_TOTAL_EX_CB;
			}
			record.setMarket(rs.getString("GFDG50_PCT_C").trim());
		} else if (productType.equals(ProductHierarchy.PCT)) {
			key = Constant.PCT;
			record.setBusinessUnit(rs.getString("GFDG50_PCT_C").trim());
		} else if (productType.equals(ProductHierarchy.CG)) {
			key = rs.getString("GFDG50_PCT_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_CG_C").trim());
		} else if (productType.equals(ProductHierarchy.MPL)) {
			String pct = rs.getString("GFDG50_PCT_C").trim();
			String cg = rs.getString("GFDG50_CG_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_MPL_C").trim());
			key = pct + cg;
		} else if (productType.equals(ProductHierarchy.MLI)) {
			String pct = rs.getString("GFDG50_PCT_C").trim();
			String cg = rs.getString("GFDG50_CG_C").trim();
			String mpl = rs.getString("GFDG50_MPL_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_MLI_C").trim());
			key = pct + cg + mpl;
		}
		if (dataMap.containsKey(key)) {
			dataMap.get(key).add(record);
		} else {
			List<DailySalesPerformanceEURecord> data = new ArrayList<DailySalesPerformanceEURecord>();
			data.add(record);
			dataMap.put(key, data);
		}
	}


	public Map<String, DailySalesPerformanceEURate> retrievePreviousYearRate(String selectedMonth) {
		Map<String, DailySalesPerformanceEURate> previousYearRate = new HashMap<String, DailySalesPerformanceEURate>();

		Connection con = null;	
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		StringBuffer sqlString = new StringBuffer();
		sqlString.append(" SELECT CASE C03.COUNTRY_ISO3_C  WHEN 'RUS' THEN 'JVC'  ");
		sqlString.append(" ELSE C03.COUNTRY_ISO3_C ");
		sqlString.append(" END    AS MARKET_C,  ");	
		sqlString.append("  P21.MONEX_B_RATE_R AS BOOKKEEPING_RATE_PY,  ");	
		sqlString.append(" C21.MONEX_G_RATE_R AS BUDGET_RATE_CY,  ");	
		sqlString.append(" E21.MONEX_B_RATE_R AS EUR_RATE_PY  ");	
		sqlString.append(" FROM " +SqlUtils.getDbInstanceView()+".SGFDF04 F04,  ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC21 C21,   ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC03 C03,   ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC21 P21,   ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC21 E21  ");	
		sqlString.append(" WHERE C03.COUNTRY_ISO3_C = F04.COUNTRY_ISO3_C ");	
		sqlString.append(" AND         F04.CONTINENT_C = 'EU'  ");	
		sqlString.append(" AND         C03.FUND_CODE_C = C21.FUND_CODE_C  ");	
		sqlString.append(" AND         C03.FUND_CODE_C = P21.FUND_CODE_C   ");	
		sqlString.append(" AND         E21.FUND_CODE_C = 'EUR'  ");	
		sqlString.append(" AND         C21.MONEX_MNTHYR_Y = ?  ");	
		sqlString.append(" AND         P21.MONEX_MNTHYR_Y = ?  ");	
		sqlString.append(" AND         E21.MONEX_MNTHYR_Y = ?  ");	
		sqlString.append(" UNION  ");	
		sqlString.append(" SELECT CASE C03.COUNTRY_ISO3_C  ");	
		sqlString.append(" WHEN 'JVC' THEN 'RUS'  ");	
		sqlString.append(" ELSE C03.COUNTRY_ISO3_C  ");	
		sqlString.append(" END    ,  ");	
		sqlString.append(" CASE C03.COUNTRY_ISO3_C WHEN 'EDM' THEN 1 ELSE P21.MONEX_B_RATE_R END,  ");	
		sqlString.append(" C21.MONEX_G_RATE_R,  ");	
		sqlString.append(" E21.MONEX_B_RATE_R  ");	
		sqlString.append(" FROM " +SqlUtils.getDbInstanceView()+".SGFDC21 C21,  ");
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC03 C03,   ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC21 P21,   ");	
		sqlString.append(SqlUtils.getDbInstanceView()+".SGFDC21 E21  ");	
		sqlString.append(" WHERE C03.COUNTRY_ISO3_C IN ('EDM','ROM','TUR','JVC') ");	
		sqlString.append(" AND         (CASE C03.COUNTRY_ISO3_C  ");	
		sqlString.append(" WHEN 'EDM' THEN 'EUR'   ");	
		sqlString.append(" ELSE C03.FUND_CODE_C  ");	
		sqlString.append(" END)      = P21.FUND_CODE_C  ");
		sqlString.append(" AND         C21.FUND_CODE_C = 'EUR'  ");	
		sqlString.append(" AND         E21.FUND_CODE_C = 'EUR'  ");	
		sqlString.append(" AND         C21.MONEX_MNTHYR_Y = ?  ");	
		sqlString.append(" AND         P21.MONEX_MNTHYR_Y = ?  ");	
		sqlString.append(" AND         E21.MONEX_MNTHYR_Y = ?  ");	

		log.info("sqlString in retrievePreviousYearRate method--> "+sqlString.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sqlString.toString());	
			int previousYear = Integer.parseInt(selectedMonth) - 100;
			pstmt.setString(1, selectedMonth);
			pstmt.setInt(2, previousYear);
			pstmt.setInt(3, previousYear);
			pstmt.setString(4, selectedMonth);
			pstmt.setInt(5, previousYear);
			pstmt.setInt(6, previousYear);
			rs = pstmt.executeQuery();					
			while (rs.next()) {
				DailySalesPerformanceEURate dsprEuRate = new DailySalesPerformanceEURate();
				dsprEuRate.setMarket(rs.getString("MARKET_C"));
				dsprEuRate.setBookKeepingRatePreviousYear(rs.getDouble("BOOKKEEPING_RATE_PY"));
				dsprEuRate.setBudgetRateCurrentYear(rs.getDouble("BUDGET_RATE_CY"));
				dsprEuRate.setEuroRatePreviousYear(rs.getDouble("EUR_RATE_PY"));
				previousYearRate.put(dsprEuRate.getMarket(), dsprEuRate);
			}

		}
		catch (SQLException e){
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, "retrievePreviousYearRate").build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs , pstmt, con );
		}
		return previousYearRate;
	}

	/**
	 * @param strMarket
	 * @return HashMap<String,List<Integer>>
	 */
	public HashMap<String,List<Integer>> retrivePostedDateDaysNWorkingDays(String strMarket){
		final String  METHOD_NAME = "retrivePostedDateDaysNWorkingDays(String)";
		log.entering(CLASS_NAME, METHOD_NAME);

		HashMap<String,List<Integer>> hMapCountryPostedDateWorkingDaysNWorkingDays = new HashMap<String,List<Integer>>();

		Connection con = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;

		ResultSet rs = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;

		StringBuilder sql = new StringBuilder();  	

		/*
    	SELECT CAST( CAST(max(GFDG32_UPDTRAN_Y)  AS DATE FORMAT 'DD')  AS CHAR(02) ) TRANSDAYS ,
    	CAST( CAST(max(GFDG32_UPDTRAN_Y)  AS DATE FORMAT 'YYYYMM')  AS CHAR(06) ) TRANYEARMONTH
    	FROM GFIN_DATA.SGFDG32 g32
    	WHERE g32.GFDA07_REGION_C = 'EU' AND GFDG32_MARKET_C = ?*/
		sql.append(" SELECT CAST( CAST(max(GFDG32_UPDTRAN_Y)  AS DATE FORMAT 'DD')  AS CHAR(02) ) TRANSDAY ,");
		sql.append(" CAST( CAST(max(GFDG32_UPDTRAN_Y)  AS DATE FORMAT 'YYYYMM')  AS CHAR(06) ) YEARMONTH");
		sql.append(" FROM "  + SqlUtils.getDbInstanceView() +  ".SGFDG32 ");    	
		sql.append(" WHERE GFDA07_REGION_C = 'EU'");
		sql.append(" AND GFDG32_MARKET_C = ? ");
		log.info(CLASS_NAME + METHOD_NAME +":::::"+sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, strMarket);
			rs = pstmt.executeQuery();	

			while (rs.next()) {  
				int postedDateDay = Integer.parseInt(rs.getString("TRANSDAY").trim());
				int yearMonth = Integer.parseInt(rs.getString("YEARMONTH").trim());

				List<Integer> lstWorkingDays = new ArrayList<Integer>(); 

				if(!"".equals(strMarket )){
					/* SELECT	SUM(WKDAY_WKDAY_R ) WORKINGDAYS  FROM GFIN_DATA.SGFDW03
    				 WHERE  GFDG73_PT_SLS_ANLYS_GRP_C ='EU'  AND  WKDAY_WKDAY_R = '1.0000'   AND WKDAY_DAY_R <=  31
    				   AND  COUNTRY_ISO3_C ='GBR' AND  TRIM(WKDAY_MNTHYR_Y) = 201808*/
					StringBuilder sql1 = new StringBuilder();
					sql1.append(" SELECT SUM(WKDAY_WKDAY_R ) WORKINGDAYS ");
					sql1.append(" FROM "  + SqlUtils.getDbInstanceView() +  ".SGFDW03 ");    	
					sql1.append(" WHERE GFDG73_PT_SLS_ANLYS_GRP_C ='EU' AND  WKDAY_WKDAY_R = '1.0000' AND WKDAY_DAY_R <=  31 ");
					sql1.append(" AND  COUNTRY_ISO3_C = ? ");
					sql1.append(" AND  TRIM(WKDAY_MNTHYR_Y) = ? ");
					log.info(CLASS_NAME + METHOD_NAME +":::::"+sql1.toString());

					pstmt1 = con.prepareStatement(sql1.toString());
					pstmt1.setString(1, strMarket);
					pstmt1.setInt(2, yearMonth);     		
					rs1 = pstmt1.executeQuery();
					while(rs1.next()){
						int workingDays = rs1.getInt("WORKINGDAYS");
						lstWorkingDays.add(workingDays);    			    	 

						StringBuilder sql2 = new StringBuilder();
						sql2.append(" SELECT SUM(WKDAY_WKDAY_R ) POSTEDDATEWORKINGDAYS ");
						sql2.append(" FROM "  + SqlUtils.getDbInstanceView() +  ".SGFDW03 ");    	
						sql2.append(" WHERE GFDG73_PT_SLS_ANLYS_GRP_C ='EU' AND  WKDAY_WKDAY_R = '1.0000' ");
						sql2.append(" AND COUNTRY_ISO3_C = ? ");
						sql2.append(" AND TRIM(WKDAY_MNTHYR_Y) = ? ");
						sql2.append(" AND WKDAY_DAY_R <=  ? ");

						log.info(CLASS_NAME + METHOD_NAME +":::::"+sql2.toString());
						pstmt2 = con.prepareStatement(sql2.toString());
						pstmt2.setString(1, strMarket);
						pstmt2.setInt(2, yearMonth);
						pstmt2.setInt(3, postedDateDay);
						rs2 = pstmt2.executeQuery();
						while(rs2.next()){	     		    			
							int postedDateWorkingDays = rs2.getInt("POSTEDDATEWORKINGDAYS");	     		    			
							lstWorkingDays.add(postedDateWorkingDays);	     		    			
						}
					}

				}
				hMapCountryPostedDateWorkingDaysNWorkingDays.put(strMarket,lstWorkingDays);

			}
		} catch (Exception e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeResultSet(rs2,pstmt2);
			closeResultSet(rs1,pstmt1);
			closeConnectionResultSet(rs , pstmt, con );
		}
		log.exiting(CLASS_NAME, METHOD_NAME);

		return hMapCountryPostedDateWorkingDaysNWorkingDays;

	}
	public Map<String, List<DailySalesPerformanceEURecord>> getTLRUnitsData(String selectedMarket, String selectedMonth,String selectedDlrChannel,
			ProductHierarchy productType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getTLRUnitsData";
		log.entering(CLASS_NAME, METHOD_NAME);

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT GFDG50_PCT_C,  ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append("GFDG50_CG_C, ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C, ");
		}
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG40_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG40_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG40_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T)/1000 grossProjMtdPy, "); // This column will not be used as on 10/11/2018 by Sanjeev as requested by Carlos
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG40_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append("group by GFDG50_PCT_C ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(", GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		sql.append("order by GFDG50_PCT_C ");	
		log.logp(Level.INFO, METHOD_NAME, productType + ":= ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs, dataMap, productType, true, "", warrantyForecast, true);

			}
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return dataMap;
	}
	public Map<String, List<DailySalesPerformanceEURecord>> getFilteredTLRUnits(String selectedMarket, String selectedMonth,
			ProductHierarchy productType, String selectedDlrChannel, String selectedDlrRegion, 
			String selectedDlrZone, String selectedDealers, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredTLRData";
		log.entering(CLASS_NAME, METHOD_NAME);



		int objectiveAtProductLevel = 0;

		boolean displayObjectives = true;
		boolean displayWarranty = true;
		if(!"".equals(selectedMarket) && !"".equals(selectedDlrChannel)){

			if (hmapObjectiveLevels.containsKey(selectedMarket)){

				HashMap<String,Integer> hmapTemp = new HashMap<String,Integer>();

				hmapTemp = hmapObjectiveLevels.get(selectedMarket);

				if(hmapTemp.containsKey(selectedDlrChannel)){			

					objectiveAtProductLevel = hmapTemp.get(selectedDlrChannel);
				}else{

					objectiveAtProductLevel = hmapTemp.get("OTHER");
				}


			}

		}
		log.info("PRODUCT TYPE VALUE ::::::::"+productType.getValue());

		if (objectiveAtProductLevel >= productType.getValue() ){

			displayObjectives = false;
		}
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT GFDG50_PCT_C,  ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append("GFDG50_CG_C, ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append("GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C, ");
		}
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
			displayWarranty = false;
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		sql.append("group by GFDG50_PCT_C ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(", GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		sql.append("order by GFDG50_PCT_C ");
		if (productType.equals(ProductHierarchy.CG)) {
			sql.append(", GFDG50_CG_C ");
		} else if (productType.equals(ProductHierarchy.MPL)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C ");
		} else if (productType.equals(ProductHierarchy.MLI)) {
			sql.append(", GFDG50_CG_C, GFDG50_MPL_C, GFDG50_MLI_C ");
		}
		log.logp(Level.INFO,METHOD_NAME, productType + ":= ", sql.toString());
		log.info("main query in TLR+++++"+ sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs, dataMap, productType, displayObjectives, "", warrantyForecast, displayWarranty);
			}
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return dataMap;
	}
	public DailySalesPerformanceEURecord getFilteredGrandTotalUnits(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredGrandTotalData";
		log.entering(CLASS_NAME, METHOD_NAME);

		boolean displayWarranty = true;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord grandTotalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'GRAND TOTAL' GFDG50_PCT_C, ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND   Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
			displayWarranty = false;
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();

			while (rs.next()) {
				processUnitResultSet(rs, dataMap, null, true, parentType, warrantyForecast, displayWarranty);
			}
			grandTotalRecord = dataMap.get(Constant.GRAND_TOTAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return grandTotalRecord;
	}


	public DailySalesPerformanceEURecord getFilteredMechanicalUnits(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredMechanicalUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		boolean displayWarranty = true;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoMechanicalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'Memo: MECHANICAL' GFDG50_PCT_C, ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND   Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		sql.append("and GFDG50_PCT_C in ('L', 'P', 'M', 'O')");
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
			displayWarranty = false;
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();

			while (rs.next()) {
				processUnitResultSet(rs, dataMap, null, true, parentType, warrantyForecast, displayWarranty);
			}
			memoMechanicalRecord = dataMap.get(Constant.MEMO_MECHANICAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoMechanicalRecord;
	}

	public DailySalesPerformanceEURecord getFilteredMemoTiresUnits(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredMemoTiresUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		boolean displayWarranty = true;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoTiresRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'Memo: TIRES' GFDG50_PCT_C, ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		sql.append(" AND GFDG50_PCT_C in ('L','B') ");
		sql.append("AND  GFDG50_MLI_C in ('0098' , '0120') ");
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
			displayWarranty = false;
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();

			while (rs.next()) {
				processUnitResultSet(rs, dataMap, null, false, parentType, warrantyForecast, displayWarranty);
			}
			if(!dataMap.isEmpty())
				memoTiresRecord = dataMap.get(Constant.MEMO_TIRES).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoTiresRecord;
	}

	public DailySalesPerformanceEURecord getFilteredGrandTotalExclCBUnits(String selectedMarket,
			String selectedMonth, String selectedDlrChannel, String selectedDlrRegion, String selectedDlrZone,
			String selectedDealers, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getFilteredGrandTotalExclCBUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		boolean displayWarranty = true;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord grandTotalExclCBRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'GRAND TOTAL excl. CB' GFDG50_PCT_C, ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}

		sql.append(" and GFDG50_PCT_C <> 'B' ");
		if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MRKT_MARKET_C=? ");
		}
		if (!selectedDlrRegion.equals(Constant.T)) {
			sql.append("and OLEV1_SA_LEVEL1_C=? ");
			displayWarranty = false;
		}
		if (!selectedDlrZone.equals(Constant.T)) {
			sql.append("and OLEV3_SA_LEVEL3_C=? ");
		}
		if (!selectedDealers.equals(Constant.T) && selectedDealers.contains(",")) {
			String[] dealers = selectedDealers.split(",");
			sql.append("and MKT_CUSTOMER_ID_C in (? ");
			for (int i = 1; i < dealers.length; i++) {
				if(i == dealers.length-1){
					sql.append(",?)");
				} else {
					sql.append(",?");
				}
			}
		} else if (!selectedDealers.equals(Constant.T)){
			sql.append("and MKT_CUSTOMER_ID_C=? ");
		}
		log.info(METHOD_NAME + sql.toString());
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			if (!selectedDlrChannel.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			if (!selectedDlrRegion.equals(Constant.T)) {
				pstmt.setString(4, selectedDlrRegion);
			}
			if (!selectedDlrZone.equals(Constant.T)) {
				pstmt.setString(5, selectedDlrZone);
			}
			if (!selectedDealers.equals(Constant.T) && !selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
				pstmt.setString(6, selectedDealers);
			} else if(selectedDlrChannel.equals(Constant.DEALER_GROUP)){
				String[] dealers = selectedDealers.split(",");
				for (int i = 0; i < dealers.length; i++) {
					pstmt.setString((3 + i), dealers[i]);
				}
			}
			rs = pstmt.executeQuery();

			while (rs.next()) {
				processUnitResultSet(rs, dataMap, null, true, parentType, warrantyForecast, displayWarranty);
			}
			grandTotalExclCBRecord = dataMap.get(Constant.GRAND_TOTAL_EX_CB).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return grandTotalExclCBRecord;
	}

	public DailySalesPerformanceEURecord getGrandTotalUnits(String selectedMarket,String selectedDlrChannel, String selectedMonth, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getGrandTotalData";
		log.entering(CLASS_NAME, METHOD_NAME);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord grandTotalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'GRAND TOTAL' GFDG50_PCT_C , ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append(" AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs,dataMap,null,true, parentType, warrantyForecast, true);

			}
			grandTotalRecord = dataMap.get(Constant.GRAND_TOTAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return grandTotalRecord;
	}


	public DailySalesPerformanceEURecord getMechanicalUnits(String selectedMarket, String selectedDlrChannel,String selectedMonth, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getMechanicalUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoMechanicalRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'Memo: MECHANICAL' GFDG50_PCT_C , ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C ='EU' ");
		sql.append(" and GFDG50_DSPLY_CTRY_C = ? ");
		sql.append(" and GFDG50_MNTHYR_Y = ? ");

		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND Substr(MrKT_market_c,5,1)   IN ('1','2')     ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append(" and GFDG50_PCT_C in ('L', 'P', 'M', 'O') ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs,dataMap,null,true, parentType, warrantyForecast, true);

			}
			memoMechanicalRecord = dataMap.get(Constant.MEMO_MECHANICAL).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoMechanicalRecord;
	}

	public DailySalesPerformanceEURecord getMemoTiresUnits(String selectedMarket, String selectedMonth,String selectedDlrChannel, String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getMemoTiresUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord memoTiresRecord = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'Memo: TIRES' GFDG50_PCT_C , ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");


		sql.append("and GFDG50_MNTHYR_Y=?  ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("   AND Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append(" AND GFDG50_PCT_C in ('L','B') ");
		sql.append("AND  gfdg50_mli_c in ('0098' , '0120') ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs,dataMap,null,false, parentType, warrantyForecast, true);

			}
			if(!dataMap.isEmpty())
				memoTiresRecord = dataMap.get(Constant.MEMO_TIRES).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return memoTiresRecord;
	}

	public DailySalesPerformanceEURecord getGrandTotalExclDBUnits(String selectedMarket,String selectedMonth, String selectedDlrChannel,String parentType, Map<String, String> warrantyForecast) {

		final String METHOD_NAME = "getGrandTotalExclDBUnits";
		log.entering(CLASS_NAME, METHOD_NAME);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		Map<String, List<DailySalesPerformanceEURecord>> dataMap = new LinkedHashMap<String, List<DailySalesPerformanceEURecord>>();
		DailySalesPerformanceEURecord getGrandTotalExclDBUnits = new DailySalesPerformanceEURecord();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT  'GRAND TOTAL excl. CB' GFDG50_PCT_C , ");
		sql.append("sum(SALES_GROSS_PART_T) mtdGrossRev, ");
		//sql.append("sum(GFDG50_BDN_PROJ_A) projGrossRev, ");//blank
		sql.append("sum(WRNTY_PIECE_PAID_T)  warranty, ");
		//sql.append("sum(GFDG50_NPRC_PROJ_A) newPrice, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PROJ_T) as grossCPVProjected, ");
		//sql.append("sum(GFDG50_CPV_OBJ_A) grossCPVObjective, ");//blank
		sql.append("sum(GFDG50_GROSS_CPV_PY_T) grossCPVPY, ");
		sql.append("sum(GFDG50_PROJ_MTD_VS_PY_T) grossProjMtdPy, ");
		sql.append("sum(GFDG50_BDN_TCYTCMD_T) TCYTCMD, ");
		//sql.append("sum(GFDG50_BDN_TCYTCMD_A) AS TCYTCMD, ");
		sql.append("sum(GFDG50_PROJ_YTD_VS_PYTD_T) TPYTPYCMD, ");
		sql.append("sum(GFDG50_BDN_LAST5_DAYS_T) last5daysAmt, ");
		sql.append("sum(GFDG50_BDN_LAST20_DAYS_T) last20daysAmt ");
		sql.append("from " + SqlUtils.getDbInstanceView() + ".SGFDG50 ");
		sql.append("WHERE GFDA07_REGION_C='EU' ");
		sql.append("and GFDG50_DSPLY_CTRY_C= ? ");
		sql.append("and GFDG50_MNTHYR_Y=? ");


		if(selectedDlrChannel.equals(Constant.T)) {
			//sql.append("    ( (GFDG50_DSPLY_CTRY_C     IN ('RUS','TUR') AND         ");
			//sql.append("   substr(MrKT_market_c,5,1)       = 'B')              OR      ");
			//sql.append("   (GFDG50_DSPLY_CTRY_C NOT IN ('RUS','TUR') AND         ");
			sql.append("  AND  Substr(MrKT_market_c,5,1)       IN ('1','2')      ");	
			//			sqlString.append("    ) )       ");
		}
		if(selectedDlrChannel.equals(Constant.DEALER_GROUP)) {
			sql.append("and MKT_CUSTOMER_ID_C= 'T' ");
		}
		sql.append(" and GFDG50_PCT_C <> 'B'  ");
		log.logp(Level.INFO, METHOD_NAME, "SQL := ", sql.toString());

		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMarket);
			pstmt.setString(2, selectedMonth);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				processUnitResultSet(rs,dataMap,null,true, parentType, warrantyForecast, true);

			}
			getGrandTotalExclDBUnits = dataMap.get(Constant.GRAND_TOTAL_EX_CB).get(0);
		} catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return getGrandTotalExclDBUnits;
	}


	private void processUnitResultSet(ResultSet rs,	Map<String, List<DailySalesPerformanceEURecord>> dataMap,
			ProductHierarchy productType, boolean objectiveDisplayFlag, String parentType, Map<String, String> warrantyForecast, boolean displayWarranty)			 throws SQLException {

		DailySalesPerformanceEURecord record = new DailySalesPerformanceEURecord();
		DecimalFormat decimalFormatter = new DecimalFormat("#,###.0;(#,###.0)");
		DecimalFormat numberFormatter = new DecimalFormat("#,###;(#,###)");
		// Fetching from Result set
		double mtdGrossRev = rs.getDouble("mtdGrossRev");
		//	double projGrossRev = rs.getDouble("projGrossRev");//blank
		double warranty = rs.getDouble("warranty");
		double grossCPVProjected = rs.getDouble("grossCPVProjected");
		//double newPrice = rs.getDouble("newPrice");//blank
		double grossCPVPY = rs.getDouble("grossCPVPY");
		double grossProjMtdPy=rs.getDouble("grossProjMtdPy");  // Commented as on 10/11/2018 by Sanjeev as requested by Carlos
		//double grossCPVObjective = rs.getDouble("grossCPVObjective");//blank
		double tcytcmd = rs.getDouble("TCYTCMD");
		double tpytpycmd = rs.getDouble("TPYTPYCMD"); // Commented as on 10/11/2018 by Sanjeev as requested by Carlos
		//	double ytd = rs.getDouble("grossCPVProjYTD");  // Commented as on 10/11/2018 by Sanjeev as requested by Carlos
		double avg5 = rs.getDouble("last5daysAmt");// Changes 08/21
		double avg20 = rs.getDouble("last20daysAmt");// Changes 08/21


		// Variable for calculation
		double projectedVsObjective = 0;
		double projectedVsPY = 0;
		double ytdVsPY = 0;
		double average5 = 0;
		double average20 = 0;
		
		boolean displayWarrantyFlag = false;
		if(!warrantyForecast.isEmpty()) {
			String warrantyFlag = warrantyForecast.get(rs.getString("GFDG50_PCT_C").trim());
			if(warrantyFlag!= null && warrantyFlag.equalsIgnoreCase("Y")) {
				displayWarrantyFlag = true;
				displayWarrantyForGrandTotal = false;
			}
		}
		
		if((productType != null && !productType.equals(ProductHierarchy.PCT) && displayWarrantyFlag) 
				|| (productType != null && !displayWarranty && displayWarrantyFlag && productType.equals(ProductHierarchy.PCT))
				|| (productType == null && !displayWarranty && !displayWarrantyForGrandTotal)) {
			warranty = 0;
			grossCPVProjected = 0;
			tcytcmd = 0;
			//grossCPVPY = 0;
			/*GrossCPVProjYTD = 0;
			projYtdVsPYTD = 0;*/
			
		}

		/*// Changes 08/21 */
		if (avg5 != 0) {
			average5 = avg5 / 5;
		}
		if (avg20 != 0) {
			average20 = avg20 / 20;
		}


		if (tpytpycmd != 0) {
			//ytdVsPY = ((tcytcmd / tpytpycmd) * 100);
			if (tpytpycmd >= 102.5) {
				record.setYtdVsPYImagePath("/resources/images/green.gif");
			} else if (tpytpycmd < 97.5) {
				record.setYtdVsPYImagePath("/resources/images/red.gif");
			} else {
				record.setYtdVsPYImagePath("/resources/images/yellow.gif");
			}
		} else {
			record.setYtdVsPYImagePath("/resources/images/yellow.gif");
		}
		if (grossCPVPY != 0) {
			projectedVsPY =  grossCPVProjected/grossCPVPY * 100;
			if (projectedVsPY >= 102.5){
				record.setImgPath("/resources/images/green.gif");
			} else if (projectedVsPY < 97.5) {
				record.setImgPath("/resources/images/red.gif");
			} else {
				record.setImgPath("/resources/images/yellow.gif");
			}
		} else {
			record.setImgPath("/resources/images/yellow.gif");
		}
		if (average20 != 0) {//Changes 08/21
			if ((average5 / average20) >= 1.025) {//Changes 08/211.076
				record.setArrowPath("/resources/images/over.gif");
			} else if ((average5 / average20) < 0.975) {//Changes 08/21
				record.setArrowPath("/resources/images/under.gif");
			} else {
				record.setArrowPath("/resources/images/right.gif");
			}
		} 
		else {
			record.setArrowPath("/resources/images/right.gif");
		}
		if (mtdGrossRev != 0.0) {
			record.setMtdGrossRev(numberFormatter.format(mtdGrossRev));
		} else {
			record.setMtdGrossRev("");
		}
		/*if (mtdGrossRev != 0) {
				if(hMapPostedDateWorkingDaysNWorkingDays.containsKey(strMarket)){
					List<Integer> workingDays = hMapPostedDateWorkingDaysNWorkingDays.get(strMarket);
					if(workingDays.size()>0){
						double monthWorkingDays = workingDays.get(0);
						double postingDateWorkingDays = workingDays.get(1);
						log.info("postingDateWorkingDays-->"+postingDateWorkingDays);
						//if(postingDateWorkingDays>0)
						projGrossRev = mtdGrossRev/ postingDateWorkingDays * monthWorkingDays ; 
					}
				}
			}*/

		if (warranty != 0.0) {
			record.setWarranty(numberFormatter.format(warranty));
		} else {
			record.setWarranty("");
		}

		if(grossCPVProjected != 0.0){
			record.setGrossCPVProjected(numberFormatter.format(grossCPVProjected));
		} else {
			record.setGrossCPVProjected("");
		}
		if(objectiveDisplayFlag){

			if (projectedVsObjective != 0.0) {   // Proj vs Obj
				record.setProjectedVsObjective(decimalFormatter.format(projectedVsObjective) + "%");
			} else {
				record.setProjectedVsObjective("");
			}
		} else {
			record.setProjectedVsObjective("");
		}
		if (grossCPVPY != 0.0) {
			record.setGrossCPVPY(numberFormatter.format(grossCPVPY));
		} else {
			record.setGrossCPVPY("");
		}

		// Commented below condition as on 10/11/2018 by Sanjeev as requested by Carlos.

		record.setProjectedVsPY("");

		if (tcytcmd != 0.0) {
			record.setYtd(numberFormatter.format(tcytcmd));
		} else {
			record.setYtd("");
		}
		record.setYtdVsPY("");

		String key = "";
		if (productType == null) {
			if(parentType.equalsIgnoreCase(Constant.GRAND_TOTAL)) {
				key = Constant.GRAND_TOTAL;
			}
			else if(parentType.equals(Constant.MEMO_MECHANICAL)) {
				key = Constant.MEMO_MECHANICAL;
			}
			else if(parentType.equals(Constant.MEMO_TIRES)) {
				key = Constant.MEMO_TIRES;
			}
			else if(parentType.equals(Constant.GRAND_TOTAL_EX_CB)) {
				key = Constant.GRAND_TOTAL_EX_CB;
			}

			record.setMarket(rs.getString("GFDG50_PCT_C").trim());
		} else if (productType.equals(ProductHierarchy.PCT)) {
			key = Constant.PCT;
			record.setBusinessUnit(rs.getString("GFDG50_PCT_C").trim());
		} else if (productType.equals(ProductHierarchy.CG)) {
			key = rs.getString("GFDG50_PCT_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_CG_C").trim());
		} else if (productType.equals(ProductHierarchy.MPL)) {
			String pct = rs.getString("GFDG50_PCT_C").trim();
			String cg = rs.getString("GFDG50_CG_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_MPL_C").trim());
			key = pct + cg;
		} else if (productType.equals(ProductHierarchy.MLI)) {
			String pct = rs.getString("GFDG50_PCT_C").trim();
			String cg = rs.getString("GFDG50_CG_C").trim();
			String mpl = rs.getString("GFDG50_MPL_C").trim();
			record.setBusinessUnit(rs.getString("GFDG50_MLI_C").trim());
			key = pct + cg + mpl;
		}
		if (dataMap.containsKey(key)) {
			dataMap.get(key).add(record);
		} else {
			List<DailySalesPerformanceEURecord> data = new ArrayList<DailySalesPerformanceEURecord>();
			data.add(record);
			dataMap.put(key, data);
		}

	}
	
	public Map<String, String> buildWarrantyForecast(String selectedMonth,
			String selectedDlrChannel, String selectedMarket) {
		final String METHOD_NAME = "buildWarrantyForecast";
		log.entering(CLASS_NAME, METHOD_NAME);
		
		Map<String, String> warrantyForecastFlag = new HashMap<String, String>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection con = null;
		
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT GFDG52_PCT_C,  ");
		sql.append("GFDG52_OVERRIDE_F  ");
		sql.append("FROM "+SqlUtils.getDbInstanceView()+".SGFDG52  ");
		sql.append("where GFDG52_MNTHYR_Y = ?  ");
		sql.append("and GFDG52_MARKET_C = ?  ");
		if(!selectedDlrChannel.equals(Constant.T)) {
			sql.append("and MRKT_MARKET_C = ? ");
		}
		
		try {
			con = getConnectionFromFactory();
			pstmt = con.prepareStatement(sql.toString());
			pstmt.setString(1, selectedMonth);
			pstmt.setString(2, selectedMarket);
			if(!selectedDlrChannel.equals(Constant.T)) {
				pstmt.setString(3, selectedDlrChannel);
			}
			
			rs = pstmt.executeQuery();
			while (rs.next()) {
				warrantyForecastFlag.put(rs.getString("GFDG52_PCT_C").trim(), rs.getString("GFDG52_OVERRIDE_F").trim()); 
			}
		}
		catch (SQLException e) {
			throw new IbisRuntimeException(new FordExceptionAttributes.Builder(
					CLASS_NAME, METHOD_NAME).build(), "",
					e.getLocalizedMessage());
		} finally {
			closeConnectionResultSet(rs, pstmt, con);
		}
		return warrantyForecastFlag;
	}

}
