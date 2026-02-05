package com.ospreydcs.dp.service.common.bson.calculations;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.types.ObjectId;

import java.util.*;

public class CalculationsDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private List<CalculationsDataFrameDocument> dataFrames;

    public ObjectId getId() {
        return id;
    }
    public void setId(ObjectId id) {
        this.id = id;
    }
    public List<CalculationsDataFrameDocument> getDataFrames() {
        return dataFrames;
    }

    public void setDataFrames(List<CalculationsDataFrameDocument> dataFrames) {
        this.dataFrames = dataFrames;
    }

    public static CalculationsDocument fromCalculations(Calculations requestCalculations) {

        final CalculationsDocument calculationsDocument = new CalculationsDocument();

        List<CalculationsDataFrameDocument> dataFrameDocuments = new ArrayList<>();
        for (Calculations.CalculationsDataFrame dataFrame : requestCalculations.getCalculationDataFramesList()) {
            CalculationsDataFrameDocument calculationsDataFrameDocument =
                    CalculationsDataFrameDocument.fromCalculationsDataFrame(dataFrame);
            dataFrameDocuments.add(calculationsDataFrameDocument);
        }
        calculationsDocument.setDataFrames(dataFrameDocuments);

        return calculationsDocument;
    }

    public Calculations toCalculations() throws DpException {
        final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
        calculationsBuilder.setId(this.getId().toString());
        for (CalculationsDataFrameDocument dataFrameDocument : getDataFrames()) {
            final Calculations.CalculationsDataFrame dataFrame = dataFrameDocument.toCalculationsDataFrame();
            calculationsBuilder.addCalculationDataFrames(dataFrame);
        }
        return calculationsBuilder.build();
    }

    public List<String> diffCalculations(Calculations calculations) {
        List<String> diffs = new ArrayList<>();
        if (calculations.getCalculationDataFramesCount() != this.getDataFrames().size()) {
            diffs.add("dataFrames count");
        } else {
            for (int i = 0; i < calculations.getCalculationDataFramesCount(); i++) {
                Calculations.CalculationsDataFrame dataFrame = calculations.getCalculationDataFrames(i);
                CalculationsDataFrameDocument dataFrameDocument = this.getDataFrames().get(i);
                try {
                    if ( ! Objects.equals(dataFrame, dataFrameDocument.toCalculationsDataFrame())) {
                        diffs.add("dataFrames[" + i + "]");
                    }
                } catch (DpException e) {
                    diffs.add("exception deserializing dataFrame[" + i + "]: " + e.getMessage());
                }
            }
        }
        return diffs;
    }

    public Map<String, List<String>> frameColumnNamesMap() {
        final Map<String, List<String>> frameColumnNamesMap = new HashMap<>();
        for (CalculationsDataFrameDocument dataFrameDocument : this.getDataFrames()) {
            final String frameName = dataFrameDocument.getName();
            final List<String> frameColumnNames = new ArrayList<>();
            for (DataColumnDocument frameColumn : dataFrameDocument.getDataColumns()) {
                frameColumnNames.add(frameColumn.getName());
            }
            frameColumnNamesMap.put(frameName, frameColumnNames);
        }
        return frameColumnNamesMap;
    }
}
