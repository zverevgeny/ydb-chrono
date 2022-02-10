RESOURCES_LIBRARY()

LICENSE(Service-Prebuilt-Tool)

OWNER(somov)

IF (OS_ANDROID)
    # Use LLD shipped with Android NDK.
    LDFLAGS("-fuse-ld=lld")
ELSEIF (USE_PREVIOUS_LLD_VERSION)
    # Use LLD 11
    IF (HOST_OS_LINUX)
        IF (HOST_ARCH_PPC64LE)
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:1843381106)
        ELSE()
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:1843327433)
        ENDIF()
    ELSEIF (HOST_OS_DARWIN)
        DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:1843327928)
    ENDIF()

    LDFLAGS("-fuse-ld=$LLD_ROOT_RESOURCE_GLOBAL/ld" "-Wl,--no-rosegment")
ELSE()
    # Use LLD 12
    IF (HOST_OS_LINUX)
        IF (HOST_ARCH_PPC64LE)
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:2283417120)
        ELSE()
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:2283360772)
        ENDIF()
    ELSEIF (HOST_OS_DARWIN)
        IF (HOST_ARCH_ARM64)
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:2283439721)
        ELSE()
            DECLARE_EXTERNAL_RESOURCE(LLD_ROOT sbr:2283429958)
        ENDIF()
    ENDIF()

    LDFLAGS("-fuse-ld=$LLD_ROOT_RESOURCE_GLOBAL/ld" "-Wl,--no-rosegment")
ENDIF()

END()
