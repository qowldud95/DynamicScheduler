package scheduling.dynamicScheduler.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConnectionDTO {
    private String ConnectionUrl;
    private String ConnectionID;
    private String ConnectionPW;
}