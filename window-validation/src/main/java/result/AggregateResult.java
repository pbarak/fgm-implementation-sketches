package fgm.windowvalidation.result;


public class AggregateResult {
    private Double queryResult;

    public AggregateResult() {
    }

    public AggregateResult(Double queryResult) {
        this.queryResult = queryResult;
    }


    public Double getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(Double queryResult) {
        this.queryResult = queryResult;
    }

}
