package DP.Transformations;

public class TransformationBuilder {
    private QueryHandler chain;

    public TransformationBuilder() {
        buildChain();
    }

    private void buildChain() {
        // chain = new OrcCommander(new OrcOfficer(new OrcSoldier(null)));
    }

    public void makeRequest(Query query) {
        chain.handleQuery(query);
    }
}
