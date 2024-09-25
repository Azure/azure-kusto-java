// package com.microsoft.azure.kusto.ingest;
//
// import com.microsoft.azure.kusto.data.Client;
// import com.microsoft.azure.kusto.data.ClientFactory;
// import com.microsoft.azure.kusto.data.KustoOperationResult;
// import com.microsoft.azure.kusto.data.KustoResultSetTable;
// import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
// import org.jetbrains.annotations.NotNull;
// import org.junit.jupiter.api.Assertions;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.Test;
// import reactor.core.publisher.Mono;
// import reactor.core.scheduler.Schedulers;
// import reactor.test.StepVerifier;
//
// import java.net.URISyntaxException;
//
// public class AsyncE2ETest {
//
// private static Client queryClient;
// private static String principalFqn;
//
// private static final String DB_NAME = System.getenv("TEST_DATABASE");
// private static final String APP_ID = System.getenv("APP_ID");
// private static final String APP_KEY = System.getenv("APP_KEY");
// private static final String TENANT_ID = System.getenv().getOrDefault("TENANT_ID", "microsoft.com");
// private static final String ENG_CONN_STR = System.getenv("ENGINE_CONNECTION_STRING");
//
// @BeforeAll
// public static void setUp() {
// principalFqn = String.format("aadapp=%s;%s", APP_ID, TENANT_ID);
// ConnectionStringBuilder engineCsb = createConnection(ENG_CONN_STR);
// engineCsb.setUserNameForTracing("Java_E2ETest_ø");
// try {
// queryClient = ClientFactory.createClient(engineCsb);
// } catch (URISyntaxException ex) {
// Assertions.fail("Failed to create query and streamingIngest client", ex);
// }
// }
//
// @Test
// void testShowPrincipalsAsync() {
// Mono<KustoOperationResult> laterResult = queryClient
// .executeQueryAsync(DB_NAME, String.format(".show database %s principals", DB_NAME), null)
// .subscribeOn(Schedulers.immediate());
// StepVerifier.create(laterResult)
// .expectNextCount(1L)
// .expectNextMatches(this::resultContainsPrincipal)
// .expectComplete()
// .verify();
// }
//
// private static @NotNull ConnectionStringBuilder createConnection(String connectionString) {
// if (APP_KEY == null || APP_KEY.isEmpty()) {
// return ConnectionStringBuilder.createWithAzureCli(connectionString);
// }
// return ConnectionStringBuilder.createWithAadApplicationCredentials(connectionString, APP_ID, APP_KEY, TENANT_ID);
// }
//
// private boolean resultContainsPrincipal(KustoOperationResult result) {
// boolean found = false;
// KustoResultSetTable mainTableResultSet = result.getPrimaryResults();
// while (mainTableResultSet.next()) {
// if (mainTableResultSet.getString("PrincipalFQN").equals(principalFqn)) {
// found = true;
// }
// }
// return found;
// }
//
// }
