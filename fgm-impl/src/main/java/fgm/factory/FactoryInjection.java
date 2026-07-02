package fgm.factory;

import fgm.common.cfg.implementation.FilePropertiesLocalStore;
import fgm.factory.specs.IFgmFactory;

public class FactoryInjection {
    private static final String factoryNameKey = "factory.name";
    private static final FilePropertiesLocalStore filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();


    public static IFgmFactory<?> createFactory() {
        try {
            String factoryName = filePropertiesLocalStore.retrieveString(factoryNameKey);
            Class<?> clazz = Class.forName(factoryName);
            return (IFgmFactory<?>) clazz.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate factory: " + filePropertiesLocalStore.retrieveString(factoryNameKey), e);
        }
    }
}
