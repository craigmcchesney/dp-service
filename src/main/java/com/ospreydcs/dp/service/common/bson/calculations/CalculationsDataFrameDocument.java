package com.ospreydcs.dp.service.common.bson.calculations;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.util.ArrayList;
import java.util.List;

public class CalculationsDataFrameDocument {

    String name;
    DataTimestampsDocument dataTimestamps;
    List<DataColumnDocument> dataColumns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public List<DataColumnDocument> getDataColumns() {
        return dataColumns;
    }

    public void setDataColumns(List<DataColumnDocument> dataColumns) {
        this.dataColumns = dataColumns;
    }

    public static CalculationsDataFrameDocument fromCalculationsDataFrame(
            Calculations.CalculationsDataFrame dataFrame
    ) {
        CalculationsDataFrameDocument dataFrameDocument = new CalculationsDataFrameDocument();

        // set frame name
        dataFrameDocument.setName(dataFrame.getName());

        // handle DataTimestamps
        DataTimestampsDocument dataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(dataFrame.getDataTimestamps());
        dataFrameDocument.setDataTimestamps(dataTimestampsDocument);

        // handle DataColumns
        List<DataColumnDocument> dataColumnDocuments = new ArrayList<>();
        for (DataColumn dataColumn : dataFrame.getDataColumnsList()) {
            DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumn);
            dataColumnDocuments.add(dataColumnDocument);
        }
        dataFrameDocument.setDataColumns(dataColumnDocuments);

        return dataFrameDocument;
    }

    public Calculations.CalculationsDataFrame toCalculationsDataFrame() throws DpException {

        final Calculations.CalculationsDataFrame.Builder dataFrameBuilder =
                Calculations.CalculationsDataFrame.newBuilder();

        dataFrameBuilder.setName(getName());

        dataFrameBuilder.setDataTimestamps(this.dataTimestamps.toDataTimestamps());

        for (DataColumnDocument dataColumnDocument : this.dataColumns) {
            dataFrameBuilder.addDataColumns(dataColumnDocument.toDataColumn());
        }

        return dataFrameBuilder.build();
    }

}
