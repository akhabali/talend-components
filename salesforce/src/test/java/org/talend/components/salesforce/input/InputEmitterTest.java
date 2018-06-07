package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.api.HttpApiHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit.http.junit5.HttpApiInject;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Suite of test for the Salesforce Input component")
@WithComponents("org.talend.components.salesforce")
//@HttpApi(useSsl = true)
public class InputEmitterTest {

//    static {
//        System.setProperty("talend.junit.http.capture", "true");
//    }

    @Injected
    private BaseComponentsHandler componentsHandler;

//    @HttpApiInject
//    private HttpApiHandler<?> httpApiHandler;

    private final static MavenDecrypter mavenDecrypter = new MavenDecrypter();

    private static Server serverWithPassword;

    private static Server serverWithSecuritykey;

    @BeforeAll
    static void init() {
        serverWithPassword = mavenDecrypter.find("salesforce-password");
        serverWithSecuritykey = mavenDecrypter.find("salesforce-securitykey");
    }

    @Test
    @DisplayName("Bad credentials case")
    void inputWithBadCredential() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId("badUser");
        datasore.setPassword("badPasswd");
        datasore.setSecurityKey("badSecurityKey");
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));
        final IllegalStateException ex = assertThrows(IllegalStateException.class, () -> Job.components()
                .component("salesforce-input", "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run());
        assertTrue(ex.getMessage().contains("Invalid username, password, security token; or user locked out"));
    }

    @Test
    @DisplayName("Module selection case [valid]")
    void inputWithModuleNameValid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setSelectColumnIds(singletonList("Name"));
        queryDataSet.setDataStore(datasore);
        queryDataSet.setCondition("Name Like '%Oil%'");
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));
        Job.components()
                .component("salesforce-input", "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }

    @Test
    @DisplayName("Module selection case [invalid]")
    void inputWithModuleNameInvalid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("invalid0");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> Job.components()
                .component("salesforce-input",
                        "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run());
        assertTrue(ex.getMessage().contains("sObject type 'invalid0'"));
    }

    @Test
    @DisplayName("Module selection with fields case [invalid]")
    void inputWithModuleNameValidAndInvalidField() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSelectColumnIds(singletonList("InvalidField10x"));
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> Job.components()
                .component("salesforce-input",
                        "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run());
        assertTrue(ex.getMessage().contains("columns { InvalidField10x } doesn't exist in module 'account'"));
    }

    @Test
    @DisplayName("Soql query selection [valid]")
    void inputWithSoqlQueryValid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("select Name from account where Name Like  '%Oil%'");
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));
        Job.components()
                .component("salesforce-input", "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }

    @Test
    @DisplayName("Soql query selection [invalid]")
    void inputWithSoqlQueryInvalid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("from account");
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> Job.components()
                .component("salesforce-input", "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run());

        assertTrue(ex.getMessage().contains("INVALID_SOQL"));
    }

    @Test
    @DisplayName("Soql query selection [empty result]")
    void inputWithSoqlQueryEmptyResult() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("select  name from account where name = 'this name will never exist $'");
        queryDataSet.setDataStore(datasore);
        final Map<String, String> configMap = configurationByExample(queryDataSet);
        final String config = configMap.keySet().stream()
                .map(k -> k + "=" + uriEncode(configMap.get(k))).collect(joining("&"));

        Job.components()
                .component("salesforce-input", "Salesforce://Input?" + config)
                .component("collector", "test://collector")
                .connections()
                .from("salesforce-input").to("collector")
                .build()
                .run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(0, records.size());
    }

    private String uriEncode(String s) {
        try {
            return URLEncoder.encode(s, "utf-8")
                    .replaceAll("\\+", "%20")
                    .replaceAll("\\%21", "!")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%28", "(")
                    .replaceAll("\\%29", ")")
                    .replaceAll("\\%7E", "~");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
