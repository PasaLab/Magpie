/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common;

import util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class QueryUtil {
	private static final Logger LOG = LoggerFactory.getLogger(QueryUtil.class);
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String ip = Config.getString("ip");



	public static LinkedHashMap<String, String> getQueries(String location, String queries) {
		LinkedHashMap<String, Supplier<InputStream>> sql = new LinkedHashMap<>();
		List<String> queryList = queries == null ? null : Arrays.asList(queries.split(","));
		if (location == null) {
			for (int i = 1; i < 100; i++) {
				String name = "q" + i + ".sql";
				ClassLoader cl = QueryUtil.class.getClassLoader();
				String path = "queries/" + name;
				if (cl.getResource(path) == null) {
					String a = "q" + i + "a.sql";
					sql.put(a, () -> cl.getResourceAsStream("queries/" + a));
					String b = "q" + i + "b.sql";
					sql.put(b, () -> cl.getResourceAsStream("queries/" + b));
				} else {
					sql.put(name, () -> cl.getResourceAsStream(path));
				}
			}
		} else {
			Stream<File> files = queryList == null ?
					Arrays.stream(requireNonNull(new File(location).listFiles())) :
					queryList.stream().map(file -> new File(location, file));
			files.forEach(file -> sql.put(file.getName(), () -> {
				try {
					return new FileInputStream(file);
				} catch (FileNotFoundException e) {
					return null;
				}
			}));
			files.close();
		}
		LinkedHashMap<String, String> ret = new LinkedHashMap<>();
		sql.forEach((name, supplier) -> {
			if (queryList == null || queryList.contains(name)) {
				InputStream in = supplier.get();
				if (in != null) {
					ret.put(name, streamToString(in));
				}
			}
		});
		return ret;
	}

	/**
	 * 给定表名和列名，计算相应列的重复率
	 * @param tableName 表名
	 * @param cols 需要计算重复率的列名组成的数组
	 * @return 重复率组成的列表
	 * @throws Exception
	 */
	public static Double[] getAvgRepetition(String tableName, Set<String> cols, String database){
        HashMap<String, Double> keyColFeature = new HashMap<>();
		Double avg = 0d;
		Double variance = 0d;
		Set<String> column = new HashSet<>();
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {

			/*hiverserver2 版本jdbc url格式*/
			LOG.info("Hive连接地址："+ String.format("jdbc:hive2://%s:10000/%s", ip, database));

			Connection con = DriverManager.getConnection(String.format("jdbc:hive2://%s:10000/%s", ip, database), "", "");
			Statement stmt = con.createStatement();
			//参数设置测试
			//boolean resHivePropertyTest = stmt
			//        .execute("SET tez.runtime.io.sort.mb = 128");

			boolean resHivePropertyTest = stmt
					.execute("set hive.execution.engine=mr");
//			stmt.execute("set hive.exec.parallel=true");
//			//stmt.execute(String.format("set mapred.job.queue.name=%s", database));
			stmt.execute("set mapred.reduce.tasks=8");
			stmt.execute("set hive.exec.reducers.max=8");
			stmt.execute("set hive.exec.parallel=true");
			stmt.execute("set hive.exec.parallel.thread.number=8");


			LOG.info(String.valueOf(resHivePropertyTest));
			//先计算总行数
			String sql = String.format("select count(1) from %s", tableName);
			LOG.info("Running: " + sql);
			ResultSet res = stmt.executeQuery(sql);

			int rows = 0;
			while (res.next()) {
				rows = res.getInt(1);
				LOG.info(String.format("表%s的总行数：%s", tableName, rows));
			}
			if(rows == 0){
				LOG.error(String.format("%s 总行数为零！！！无法计算相关联列的重复率~~~", tableName));
				return new Double[0];
			}
			//再过滤列名

			sql = "describe " + tableName;
			LOG.info("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				String col = res.getString(1);
				if(cols.contains(col))
					column.add(col);
			}
			LOG.info(tableName + "："+ column);
			if(column.size() == 0){
				LOG.error(String.format("%s 查询列为零！！！无法计算相关联列的重复率~~~", tableName));
				return new Double[0];
			}
			//在计算每列的重复率
			StringBuilder sb = new StringBuilder();
			sb.append("select ");
			for (String col : column) {
				sb.append(String.format("count(distinct %s),", col));
				//sql = String.format("select count(distinct(%s)) from %s" , col, tableName);

			}
			sb.delete(sb.length()-1, sb.length());
			sb.append(String.format(" from %s", tableName));
			sql = sb.toString();
			LOG.info("Running: " + sql);
			try {
				//这个SQL的是计算每列的key重复率，结果是一行多列
				res = stmt.executeQuery(sql);
				int row = 0;
				while (res.next()) {
					for(int i = 1; i <= column.size(); i++){
						row = res.getInt(i);
						if(row == 0)
							continue;
						//基数
						avg += rows/ (double) row;
					}
					//平均基数
					avg /= column.size();
					for(int i = 1; i <= column.size(); i++){
						row = res.getInt(i);
						if(row == 0)
							continue;
						variance += Math.pow(avg - rows/ (double) row, 2);
					}
					//平均基数的方差
					variance /= column.size();
				}
			}catch (Exception e){
				LOG.error(e.toString());
			}
		}catch (SQLException e){
			LOG.error(e.getSQLState()+ ":" + e.toString());
		}

		return new Double[]{avg, variance};
	}

	private static String streamToString(InputStream inputStream) {
		BufferedInputStream in = new BufferedInputStream(inputStream);
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		try {
			int c;
			while ((c = in.read()) != -1) {
				outStream.write(c);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				in.close();
			} catch (IOException ignored) {
			}
		}
		return new String(outStream.toByteArray(), StandardCharsets.UTF_8);
	}
}
