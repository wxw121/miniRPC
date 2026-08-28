package com.minirpc.spring;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DocumentedConfigurationTest {

    @Test
    void everyDocumentedConfigurationTemplateLoadsAsYaml() throws Exception {
        List<String> paths = List.of(
                "rpc-sample-server/src/main/resources/application.yml",
                "rpc-sample-client/src/main/resources/application.yml",
                "docs/examples/tls/application.yml",
                "docs/examples/mtls/application.yml");
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();

        for (String path : paths) {
            Resource resource = projectFile(path);
            assertTrue(resource.exists(), () -> "Missing documented configuration: " + path);
            assertFalse(loader.load(path, resource).isEmpty(), () -> "No YAML properties in: " + path);
        }
    }

    @Test
    void tlsTemplatesDeclareTheServerTlsSwitch() throws Exception {
        YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
        for (String path : List.of("docs/examples/tls/application.yml", "docs/examples/mtls/application.yml")) {
            Resource resource = projectFile(path);
            Object enabled = loader.load(path, resource).get(0).getProperty("minirpc.server.tls.enabled");
            assertTrue("true".equals(String.valueOf(enabled)));
        }
    }

    private static Resource projectFile(String relativePath) {
        Path directPath = Path.of(relativePath);
        Path projectPath = Files.exists(directPath)
                ? directPath
                : Path.of("..").resolve(relativePath).normalize();
        return new FileSystemResource(projectPath);
    }
}
