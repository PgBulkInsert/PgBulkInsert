package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Type;
import java.util.List;

public class ListValueHandler<TElement> extends BaseValueHandler<List<TElement>> {
    private int nDims;
    private int elementOid;
    private IValueHandler valueHandler;

    public ListValueHandler(int nDims, int elementOid, IValueHandler valueHandler) {
        this.nDims = nDims;
        this.elementOid = elementOid;
        this.valueHandler = valueHandler;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final List<TElement> value) throws Exception {
        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutputStream arrayOutput = new DataOutputStream(byteArrayOutput);

        arrayOutput.writeInt(nDims);
        arrayOutput.writeInt(0); // TODO flags?
        arrayOutput.writeInt(elementOid);

        List<?> currentDim = value;
        for (int dimI = 0; dimI < nDims; dimI++) {
            arrayOutput.writeInt(currentDim.size());
            arrayOutput.writeInt(1); // lower bound

            if (dimI < nDims - 1) {
                currentDim = (List<?>)currentDim.get(0);
            }
        }

        writeArray(arrayOutput, value, nDims - 1);

        buffer.writeInt(byteArrayOutput.size());
        buffer.write(byteArrayOutput.toByteArray());
    }

    private void writeArray(DataOutputStream buffer, final List<?> values, int remainingDims) throws Exception {
        if (remainingDims > 0) {
            for (List<?> subList: (List<List<?>>)values) {
                writeArray(buffer, subList, remainingDims - 1);
            }
        } else {
            for (TElement value: (List<TElement>)values) {
                valueHandler.handle(buffer, value);
            }
        }
    }

    @Override
    public Type getTargetType() {
        return List.class;
    }
}
