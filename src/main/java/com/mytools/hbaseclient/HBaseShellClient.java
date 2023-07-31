package com.mytools.hbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HBaseShellClient {
    static final private Logger LOG = LoggerFactory.getLogger("com.wesure.HBaseShellClient");

    private static Admin admin;
    static String action;
    static String namespace;
    static String tableName;
    static TableName fullTableName;
    static String startRow;
    static String endRow;
    static String columnFamily;
    static String column;
    static String limit;
    static String outfile;
    static Size beforeMergeMaxSize;
    static Size afterMergeMaxSize;
    static int maxSizePerGroup;
    static String DEFAULT_START_ROWKEY_VALUE="__allRows__";
    static String DEFAULT_END_ROWKEY_VALUE="__allRows__";

    static String DEFAULT_COLUMN_FAMILY_VALUE="__allCfs__";
    static String DEFAULT_COLUMN_VALUE="__allColumns__";

    static Size BEFORE_MERGE_MAX_REGION_SIZE= new Size((Double.valueOf(5)), Size.Unit.MEGABYTE);
    static Size AFTER_MERGE_MAX_REGION_SIZE=new Size((Double.valueOf(10)), Size.Unit.MEGABYTE);

    public ArrayList<String> tableList = new ArrayList<>(Arrays.asList("list","list_prefix"));
    public ArrayList<String> mergeList = new ArrayList<>(Arrays.asList("merge","Tmerge"));
    public ArrayList<String> truncateList = new ArrayList<>(Arrays.asList("truncate","truncate_preserve"));

    public HBaseShellClient(String[] args) {
        this.action=args[0];
        this.namespace=args[1];
        this.tableName=args[2];
        this.startRow=args[3];
        this.endRow=args[4];
        this.columnFamily=args[5];
        this.column=args[6];
        this.limit=args[7];
        this.outfile=args[8];
        this.beforeMergeMaxSize=args[9].isEmpty()?BEFORE_MERGE_MAX_REGION_SIZE: new Size((Double.valueOf(args[9])), Size.Unit.MEGABYTE);
        this.afterMergeMaxSize=args[10].isEmpty()?AFTER_MERGE_MAX_REGION_SIZE: new Size((Double.valueOf(args[10])), Size.Unit.MEGABYTE);
        this.maxSizePerGroup = Integer.parseInt(args[11])%1==0 && Integer.parseInt(args[11])>0 ? Integer.parseInt(args[11]):2;
        fullTableName = this.namespace.equals("default")?TableName.valueOf(this.tableName):TableName.valueOf(this.namespace,this.tableName);

    }
    public static void main(String[] args) throws IOException, HBaseException {
        if (args.length >= 8) {
            HBaseShellClient client = new HBaseShellClient(args);
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            if (admin.tableExists(fullTableName) && !client.action.equals("list")){
            Table table = connection.getTable(fullTableName);

            BufferedWriter writer = new BufferedWriter(new FileWriter(client.outfile,true));
            if (client.action.equals("get")) {
                Get get = new Get(Bytes.toBytes(client.startRow));
                get = handleColumnFamily(get,client);
                Result result = table.get(get);
                CellScanner cellScanner = result.cellScanner();
                printResult(cellScanner, writer);
            } else if (client.action.equals("scan")) {
                Scan scan = new Scan();
                if (!client.startRow.equals(DEFAULT_START_ROWKEY_VALUE)) {
                    scan.withStartRow(Bytes.toBytes(client.startRow));
                }
                if (!client.endRow.equals(DEFAULT_END_ROWKEY_VALUE)) {
                    scan.withStopRow(Bytes.toBytes(client.endRow));
                }
                scan.setLimit(Integer.parseInt(client.limit));
                scan = handleColumnFamily(scan,client);
                ResultScanner scanner = table.getScanner(scan);
                Iterator<Result> iterator = scanner.iterator();
                while (iterator.hasNext()) {
                    Result result = iterator.next();
                    CellScanner cellScanner = result.cellScanner();
                    printResult(cellScanner, writer);
                }
                scanner.close();
            } else if (client.mergeList.contains(client.action)){
                merge(client,admin,connection);
            } else if (client.action.equals("alter")){
                alterTable(client,admin,connection);
            } else if (client.action.equals("major_compact")){
                majorCompact(admin);
            } else if (client.truncateList.contains(client.action)){
                boolean flag = client.action.equals("truncate_preserve")? true : false;
                truncateTable(admin,flag);
            } else if (client.action.equals("put")) {
                putTable(client,admin,table);
            } else if (client.action.equals("flush")) {
                flush(admin);
            } else {
                LOG.error("do not support this action...");
            }
            table.close();
            if (writer != null) {
                writer.close();
            }
            } else if (client.tableList.contains(client.action)) {
                listTable(admin);
            }
            else{
                LOG.info(fullTableName.getNameWithNamespaceInclAsString()+" not exists");
            }
            admin.close();
            connection.close();
        } else {
            LOG.info("missing enough parameters...");
        }
    }

    private static Get handleColumnFamily(Get get, HBaseShellClient client) throws IOException, HBaseException {
        LOG.info(client.columnFamily);
        LOG.info(client.column);
        if (!client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && client.column.equals(DEFAULT_COLUMN_VALUE)) {
            get.addFamily(Bytes.toBytes(client.columnFamily));
        } else if (!client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && !client.column.equals(DEFAULT_COLUMN_VALUE)) {
            for (String i : client.column.split(",")) {
                get.addColumn(Bytes.toBytes(client.columnFamily), Bytes.toBytes(i));
            }
        }
        else if (client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && !client.column.equals(DEFAULT_COLUMN_VALUE)){
            LOG.info("-c, -f 参数异常");
        }else if (client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && client.column.equals(DEFAULT_COLUMN_VALUE)){
            ;
        }
        return get;
    }
    private static Scan handleColumnFamily(Scan scan, HBaseShellClient client) throws IOException, HBaseException {
        if (!client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && client.column.equals(DEFAULT_COLUMN_VALUE)) {
            scan.addFamily(Bytes.toBytes(client.columnFamily));
        } else if (!client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && !client.column.equals(DEFAULT_COLUMN_VALUE)) {
            for (String i : client.column.split(",")) {
                scan.addColumn(Bytes.toBytes(client.columnFamily), Bytes.toBytes(i));
            }
        }
        else if (client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && !client.column.equals(DEFAULT_COLUMN_VALUE)){
            LOG.info("-c, -f 参数异常");
        }else if (client.columnFamily.equals(DEFAULT_COLUMN_FAMILY_VALUE) && client.column.equals(DEFAULT_COLUMN_VALUE)){
            ;
        }
        return scan;
    }

    private static void printResult(CellScanner cellScanner, BufferedWriter writer) throws IOException {
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<String> columnValues = new ArrayList<>();
        String lastRow = null;
        Integer maxColLength= 10;
        Integer maxValLength= 10;
        String rowKey = null;
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
//            rowKey = Bytes.toString(CellUtil.cloneRow(cell));

            String rowKeyBin = Bytes.toStringBinary(CellUtil.cloneRow(cell));
            rowKey = new String(rowKeyBin.getBytes(StandardCharsets.ISO_8859_1), "UTF-8");

            String columnName = Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell));
//            String columnValue = Bytes.toString(CellUtil.cloneValue(cell));

            String cellBin = Bytes.toStringBinary(CellUtil.cloneValue(cell));
            String columnValue = new String(cellBin.getBytes(StandardCharsets.ISO_8859_1), "UTF-8");
//            String columnValue = new String(CellUtil.cloneValue(cell), "UTF-8");

            columnNames.add(columnName);
            columnValues.add(columnValue);
            Integer columnLength = columnName.length();
            Integer valueLength = columnValue.length();
            if (columnLength > maxColLength) {
                maxColLength = columnLength;
            }
            if (valueLength > maxValLength) {
                maxValLength = valueLength;
            }

            if (columnNames.size() >= 50) {
                formatResult(rowKey,columnNames, columnValues, maxColLength, maxValLength, writer);
                columnNames = new ArrayList<>();
                columnValues = new ArrayList<>();
            }
        }
        if (!(rowKey == null)){
            formatResult(rowKey,columnNames, columnValues, maxColLength, maxValLength, writer);
    }
    }

    private static void formatResult(String rowKey,ArrayList<String> columnNames, ArrayList<String> columnValues, Integer maxColLength, Integer maxVolLength, BufferedWriter writer)throws IOException {
        if (columnNames.size() != columnValues.size()) {
            throw new IllegalArgumentException("column和value个数不匹配");
        }
        StringBuilder sb = new StringBuilder();
        String header = "----------rowKey: " + rowKey + "-----------\n";
        sb.append(header);
        for (int i = 0; i < columnNames.size(); i++) {
            String row = String.format("| %-"+maxColLength + "s | %-"+maxVolLength +"s\n", columnNames.get(i), columnValues.get(i));
            sb.append(row);
        }
        String output = sb.toString();
        if (writer != null) {
            writer.write(output);
        } else {
            LOG.info(output);
        }
    }

    private static void merge(HBaseShellClient client,Admin admin, Connection connection)throws IOException {
        List<RegionInfo> regionInfos = admin.getRegions(fullTableName);
        Comparator<RegionInfo> comparator = (ri1, ri2) -> Bytes.compareTo(ri1.getStartKey(), ri2.getStartKey());
        Collections.sort(regionInfos, comparator);

        List<RegionMetrics> regionMetricsList = new ArrayList<>();
        Collection<ServerName> regionServerList = admin.getRegionServers();
        for (ServerName regionServer : regionServerList) {
            regionMetricsList.addAll(admin.getRegionMetrics(regionServer, fullTableName));
        }
        Map<String, RegionMetrics> regionMetricsMap = new HashMap<>();
        LOG.info("total region number is:" + regionMetricsList.size());
        LOG.info("region encodename are:");
        for (RegionMetrics regionMetrics : regionMetricsList) {
            String encodeName= regionMetrics.getNameAsString().split("\\.",3)[1];
            regionMetricsMap.put(encodeName, regionMetrics);
            LOG.info(encodeName);
        }
        String delimiter = "\n";
        Double currentGroupSize = 0.0;
        List<RegionInfo> currentGroup = new ArrayList<>();
        LOG.info("begin regionInfo...");
        Boolean flag = false;
        int mergeNum=0;
        int groupSize=0;
        for (RegionInfo regionInfo : regionInfos) {
            LOG.info(regionInfo.getRegionNameAsString());
            RegionMetrics rm = regionMetricsMap.get(regionInfo.getEncodedName());
            Double regionSize = rm.getStoreFileSize().get(Size.Unit.MEGABYTE);
            if (regionSize < beforeMergeMaxSize.get(Size.Unit.MEGABYTE)
                    && currentGroupSize + regionSize < afterMergeMaxSize.get(Size.Unit.MEGABYTE)) {
                if (currentGroup.isEmpty()) {
                    currentGroup.add(regionInfo);
                    currentGroupSize += regionSize;
                    flag = false;
                } else {
                    RegionInfo last = currentGroup.get(currentGroup.size() - 1);
                    if (RegionInfo.areAdjacent(last,regionInfo)){
                        currentGroup.add(regionInfo);
                        currentGroupSize += regionSize;
                        flag = false;
                    }else{
                        flag = true;
                    }
                }
            }else {
                flag = true;
            }
            if((currentGroup.size()>1 && flag) || currentGroup.size()>=maxSizePerGroup){
                groupSize = groupSize + currentGroup.size();
                mergeNum = mergeNum + 1;
                StringJoiner joiner = new StringJoiner(delimiter);
                currentGroup.forEach(item -> joiner.add(item.getRegionNameAsString()));
                LOG.info("this merge Group is:\n" + joiner.toString());

                if (client.action.equals("merge")) {
                    doMergeJob(admin,currentGroup);
                }
                LOG.info("current Group will be empty,for next region");
                currentGroupSize = 0.0;
                currentGroup = new ArrayList<>();
                if (regionSize < beforeMergeMaxSize.get(Size.Unit.MEGABYTE) && flag){
                    currentGroup.add(regionInfo);
                    currentGroupSize += regionSize;
                }
            }else if (currentGroup.size()==1 && flag){
                currentGroupSize = 0.0;
                currentGroup = new ArrayList<>();
            }
        }
        if (currentGroup.size()>1){
            groupSize = groupSize + currentGroup.size();
            mergeNum = mergeNum + 1;
            StringJoiner joiner = new StringJoiner(delimiter);
            currentGroup.forEach(item -> joiner.add(item.getRegionNameAsString()));
            LOG.info("this last merge Group is:\n" + joiner.toString());
            if (client.action.equals("merge")) {
                doMergeJob(admin,currentGroup);
            }
        }
        int lastNum = regionMetricsList.size() - groupSize + mergeNum;
        LOG.info("this generator will do merge " + mergeNum + " times");
        LOG.info("this generator after merge,will have " + lastNum + " regions");
    }

    private static void doMergeJob(Admin admin,List<RegionInfo> currentGroup) throws IOException {
        // 等待Region合并完成
        waitForMergeCompletion(admin);
        LOG.info("will begin do merge job");
        admin.mergeRegionsAsync(currentGroup.get(0).getEncodedNameAsBytes(),
                currentGroup.get(currentGroup.size() - 1).getEncodedNameAsBytes(), false);
        // 等待Region合并完成
        waitForMergeCompletion(admin);
    }

    private static void waitForMergeCompletion(Admin admin) throws IOException {
        boolean mergeInProgress = true;
        while (mergeInProgress) {
            try {
                Thread.sleep(5000);
                mergeInProgress = admin.getClusterMetrics().getRegionStatesInTransition().size() > 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    private static void alterTable(HBaseShellClient client,Admin admin, Connection connection) throws IOException, HBaseException {
        if (!client.columnFamily.equals("__allCfs__")) {
            modifyColumnFamilyDescriptor(client, admin, fullTableName);
        }else {
            if(client.startRow.equals("CONFIGURATION")) {
                TableDescriptor tableDescriptor = admin.getDescriptor(fullTableName);
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableDescriptor);
                if (client.endRow.equals("SPLIT_POLICY")) {
                    tableDescriptorBuilder.setRegionSplitPolicyClassName(client.limit);
                }
                // Set custom configuration property
                admin.modifyTable(tableDescriptor);
        }

        }
        LOG.info("alter table:"+ fullTableName + " success!!!");
    }

    private static void modifyColumnFamilyDescriptor (HBaseShellClient client,Admin admin, TableName table) throws IOException, HBaseException {
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(client.columnFamily));
        String[] attrs = client.endRow.split(",");
        String[] values = client.limit.split(",");
        for (int i=0 ; i< attrs.length; i++) {
            if (attrs[i].equals("TTL")) {
                columnFamilyDescriptorBuilder.setTimeToLive(values[i]);
            } else if (attrs[i].equals("DATA_BLOCK_ENCODING")) {
                columnFamilyDescriptorBuilder.setDataBlockEncoding(DataBlockEncoding.valueOf(values[i]));
            } else if (attrs[i].equals("BLOOMFILTER")) {
                columnFamilyDescriptorBuilder.setBloomFilterType(BloomType.valueOf(values[i]));
            } else if (attrs[i].equals("COMPRESSION")) {
                columnFamilyDescriptorBuilder.setCompressionType(Compression.Algorithm.valueOf(values[i]));
            } else if (attrs[i].equals("VERSIONS")) {
                columnFamilyDescriptorBuilder.setMaxVersions(Integer.parseInt(values[i]));
            } else if (attrs[i].equals("MIN_VERSIONS")) {
                    columnFamilyDescriptorBuilder.setMinVersions(Integer.parseInt(values[i]));
            } else {
                LOG.info("can not set attr:"+ attrs[i] + "only is DATA_BLOCK_ENCODING,MAXVERSIONS,COMPRESSION,BLOOMFILTER");
            }
//                .setBlocksize(64 * 1024)
//                .setMaxVersions(1)
        }
//        if(client.startRow.equals("CONFIGURATION")) {
//            columnFamilyDescriptorBuilder.setConfiguration(client.endRow,client.limit);
//        }
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();

        admin.modifyColumnFamily(table, columnFamilyDescriptor);
    }
    private static void majorCompact(Admin admin) throws IOException, HBaseException {
        admin.majorCompact(fullTableName);
        LOG.info("majorCompact table:"+ fullTableName + " success!!!");
    }
    private static void truncateTable(Admin admin,Boolean flag) throws IOException, HBaseException {
        admin.disableTable(fullTableName);
        admin.truncateTable(fullTableName,flag);
        if (admin.isTableDisabled(fullTableName)) {
            admin.enableTable(fullTableName);
        }
        if (admin.isTableAvailable(fullTableName)) {
            LOG.info((flag ? "truncate_preserve" : "truncate") + " table:" + fullTableName + " success!!!");
        } else {
            LOG.info((flag ? "truncate_preserve" : "truncate") + " table:" + fullTableName + " failed!!!");
        }
    }

    private static void listTable(Admin admin) throws IOException{
        TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
        String splitLine = "-------------user table start: ------------------";
        System.out.println(splitLine);
        for (TableName tName : tableNames) {
            System.out.println(tName.getNameAsString());
        }
        splitLine = "-------------user table end: ------------------";
        System.out.println(splitLine);
    }

    private static void putTable(HBaseShellClient client,Admin admin,Table table) throws IOException{
        if (client.outfile.trim().equals("|")) {
            Put put = new Put(Bytes.toBytes(client.startRow));

            put.addColumn(Bytes.toBytes(client.columnFamily), Bytes.toBytes(client.column), Bytes.toBytes(client.limit));
            table.put(put);
        }else{
            try (BufferedReader br = new BufferedReader(new FileReader(client.outfile))) {
                List<Put> puts = new ArrayList<>();
                String line;
                int k=0;
                int n=0;
                int m=0;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    k=k+1;
                    m=m+1;
                    // 处理每一行数据
                    if (values.length<3){
                        LOG.info("row:"+ m + " format is not compliant, at least 3 columns are required, such as r1, c1, value or r1, c1, value, ts1.");
                        continue;
                    }
                    Put put = new Put(Bytes.toBytes(values[0]));
                    String[] cols = values[1].split(":");
                    if (cols.length>1){
                        put.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]), Bytes.toBytes(values[2]));
                    }else{
                        put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILY_VALUE), Bytes.toBytes(cols[0]), Bytes.toBytes(values[2]));
                    }
                    if(k < client.maxSizePerGroup) {
                        puts.add(put);
                    }else{
                        table.put(puts);
                        puts = new ArrayList<>();
                        n=n+1;
                        LOG.info("table:" + fullTableName + " finished " + n + "st " + k + " rows put");
                        k=0;
                    }
                }
                if (puts.size()>0){
                    table.put(puts);
//                    puts = new ArrayList<>();
                    n=n+1;
                    LOG.info("table:" + fullTableName + " finished " + n + "st " + k + " rows put");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void flush(Admin admin) throws IOException, HBaseException {
        admin.majorCompact(fullTableName);
        LOG.info("flush table:"+ fullTableName + " success!!!");
    }
}
