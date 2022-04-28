package scheduling.dynamicScheduler.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import scheduling.dynamicScheduler.dto.ConnectionDTO;
import scheduling.dynamicScheduler.repository.TableRepository;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ScheduleTask {
    @Autowired
    private TableRepository tableRepository;


    //@Scheduled(cron = "2 * * * * *")
    public void task1() throws SQLException {

        List<Map<String, Object>> tableMapping = tableRepository.tableData("SELECT TABLE_KEY, SOURCE_TABLENAME, TARGET_TABLENAME, SOURCE_CONN, SOURCE_ID, SOURCE_PASSWORD, TARGET_CONN, TARGET_ID, TARGET_PASSWORD FROM TABLE_MAPPING");
        List<Map<String, Object>> columnMapping = tableRepository.tableData("SELECT TABLE_KEY, SOURCE_COLUMNNAME, TARGET_COLUMNNAME, IS_PRIMARYKEY FROM COLUMN_MAPPING");

        for (int i = 0; i < tableMapping.size(); i++) {
            Map<String, Object> table = tableMapping.get(i);
            String targetTableName = String.valueOf(table.get("TARGET_TABLENAME"));

            ConnectionDTO sourceConnect = new ConnectionDTO(String.valueOf(table.get("SOURCE_CONN")), String.valueOf(table.get("SOURCE_ID")), String.valueOf(table.get("SOURCE_PASSWORD")));
            ConnectionDTO targetConnect = new ConnectionDTO(String.valueOf(table.get("TARGET_CONN")), String.valueOf(table.get("TARGET_ID")), String.valueOf(table.get("TARGET_PASSWORD")));

            List<Map<String, Object>> filterColumn = columnMapping.stream().filter(x -> x.get("TABLE_KEY").equals(table.get("TABLE_KEY"))).collect(Collectors.toList());
            Map<String, Object> pkColumn = filterColumn.stream().filter(x -> (boolean) x.get("IS_PRIMARYKEY")).findFirst().get();

            List<String> sourceColumnName = new ArrayList<>();
            List<String> targetColumnName = new ArrayList<>();
            String sourceColumns = "";
            String targetColumns = "";

            for(int k = 0; k < filterColumn.size(); k++){
                sourceColumnName.add(String.valueOf(filterColumn.get(k).get("SOURCE_COLUMNNAME")));
                targetColumnName.add(String.valueOf(filterColumn.get(k).get("TARGET_COLUMNNAME")));
            }
            for(int k = 0; k < sourceColumnName.size(); k++){
                if(sourceColumns == "" && targetColumns == ""){
                    sourceColumns += sourceColumnName.get(k);
                    targetColumns += targetColumnName.get(k);
                } else {
                    sourceColumns += ", " + sourceColumnName.get(k);
                    targetColumns += ", " + targetColumnName.get(k);
                }
            }

            String sourceSql = "SELECT " + sourceColumns + " FROM " + String.valueOf(table.get("SOURCE_TABLENAME"));
            String targetSql = "SELECT " + targetColumns + " FROM " + String.valueOf(table.get("TARGET_TABLENAME"));

            List<Map<String, Object>> sourceTable = tableRepository.tableData(sourceSql, sourceConnect);
            List<Map<String, Object>> targetTable = tableRepository.tableData(targetSql, targetConnect);

            for (int j = 0; j < sourceTable.size(); j++) {
                Map<String, Object> source = sourceTable.get(j);

                if(targetTable == null || targetTable.stream().filter(x -> String.valueOf(x.get(String.valueOf(pkColumn.get("TARGET_COLUMNNAME")))).equals(String.valueOf(source.get(String.valueOf(pkColumn.get("SOURCE_COLUMNNAME")))))).count() == 0){
                    tableRepository.targetDataInsert(source, targetTableName, targetColumnName, sourceColumnName, targetConnect);
                } else {
                    tableRepository.targetDataUpdate(source, targetTableName, targetColumnName, sourceColumnName, pkColumn, targetConnect);
                }
            }
        }
    }
}