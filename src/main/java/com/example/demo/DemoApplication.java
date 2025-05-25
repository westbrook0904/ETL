package com.example.demo;

import jakarta.ws.rs.core.Response;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;

import org.keycloak.adapters.KeycloakDeploymentBuilder;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.admin.client.token.TokenManager;
import org.keycloak.common.VerificationException;
import org.keycloak.protocol.oidc.client.authentication.ClientCredentialsProviderUtils;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.representations.adapters.config.AdapterConfig;
import org.keycloak.representations.idm.UserRepresentation;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class DemoApplication {
    private static void register() {

    }
    private static void getToken() {
        String serverUrl = "http://localhost:8080/realms/master/protocol/openid-connect/token";
        String clientId = "test-admin";
        String clientSecret = "aZ22aO8pFfZU5GHC2oRX4ynktTNcIpXG";

        // 构建 HTTP POST 请求
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(serverUrl);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

            // 构建请求体参数
            String requestBody = "grant_type=client_credentials" +
                    "&client_id=" + clientId +
                    "&client_secret=" + clientSecret;

            httpPost.setEntity(new StringEntity(requestBody));
            // 发送请求
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();

                if (entity != null) {
                    // 将响应解析为 JSON
                    JSONObject jsonResponse = new JSONObject(EntityUtils.toString(entity));
                    String accessToken = jsonResponse.getString("access_token");
                    Keycloak keycloak = Keycloak.getInstance("http://localhost:8080/", "master", "test-admin", accessToken);
                    // 获取 RealmResource
                    RealmResource realmResource = keycloak.realm("master");
                    // 获取 UsersResource
                    UsersResource usersResource = realmResource.users();
                    // 创建 UserRepresentation 对象
                    UserRepresentation user = new UserRepresentation();
                    user.setUsername("test2");
                    user.setEmail("newuser@example.com");
                    user.setEnabled(true);
                    // 设置其他用户属性...

                    // 在 Keycloak 中创建用户
                    Response response1 = usersResource.create(user);
                    System.out.println("Role created successfully.");
                    // 打印访问令牌
                    System.out.println("Access Token: " + accessToken);
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        getToken();
        register();
    }

}
