{ config, lib, pkgs, ... }:

let
  cfg = config.services.zerofs;
  settingsFormat = pkgs.formats.toml { };

  # Collect unix socket directories that need to be created
  socketDirs = lib.unique (lib.filter (x: x != null) [
    (if cfg.settings.servers.ninep != null then lib.mapNullable dirOf cfg.settings.servers.ninep.unix_socket else null)
    (if cfg.settings.servers.nbd != null then lib.mapNullable dirOf cfg.settings.servers.nbd.unix_socket else null)
    (if cfg.settings.servers.rpc != null then lib.mapNullable dirOf cfg.settings.servers.rpc.unix_socket else null)
  ]);

  # Filter out null values from the settings
  filterNulls =
    attrs:
    lib.filterAttrsRecursive (_: v: v != null) (
      lib.mapAttrs (
        _: v:
        if lib.isAttrs v then filterNulls v else v
      ) attrs
    );

  # Build the configuration from module options
  configFile = settingsFormat.generate "zerofs.toml" (filterNulls cfg.settings);

in {
  options.services.zerofs = {
    enable = lib.mkEnableOption "ZeroFS, a filesystem that makes S3 your primary storage";

    package = lib.mkPackageOption pkgs "zerofs" { };

    readOnly = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Run ZeroFS in read-only mode. Read-only instances automatically see
        updates from the writer and return EROFS errors for write operations.
      '';
    };

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        Environment file to load before starting ZeroFS. Useful for secrets
        like encryption passwords and cloud credentials that can be referenced
        in the config using `''${VAR}` syntax.
      '';
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        freeformType = settingsFormat.type;

        options = {
          cache = {
            dir = lib.mkOption {
              type = lib.types.path;
              default = "/var/cache/zerofs";
              description = "Directory for caching data.";
            };

            disk_size_gb = lib.mkOption {
              type = lib.types.numbers.positive;
              default = 10.0;
              description = "Maximum disk cache size in GB.";
            };

            memory_size_gb = lib.mkOption {
              type = lib.types.nullOr lib.types.numbers.positive;
              default = null;
              description = "Memory cache size in GB.";
            };
          };

          storage = {
            url = lib.mkOption {
              type = lib.types.str;
              example = "s3://my-bucket/zerofs-data";
              description = ''
                Storage backend URL. Supports:
                - S3: `s3://bucket/path`
                - Azure: `azure://container/path`
                - GCS: `gs://bucket/path`
                - Local: `file:///path/to/storage`
              '';
            };

            encryption_password = lib.mkOption {
              type = lib.types.str;
              example = "\${ZEROFS_PASSWORD}";
              description = ''
                Encryption password for data at rest. Use environment variable
                substitution (e.g., `''${ZEROFS_PASSWORD}`) to avoid storing
                secrets in the Nix store.
              '';
            };
          };

          filesystem = lib.mkOption {
            type = lib.types.nullOr (lib.types.submodule {
              options = {
                max_size_gb = lib.mkOption {
                  type = lib.types.nullOr lib.types.numbers.positive;
                  default = null;
                  description = ''
                    Filesystem size limit in GB. When reached, writes return ENOSPC.
                    If not specified, defaults to 16 EiB (effectively unlimited).
                  '';
                };

                compression = lib.mkOption {
                  type = lib.types.nullOr lib.types.str;
                  default = null;
                  example = "zstd-3";
                  description = ''
                    Compression algorithm. Options:
                    - `lz4` (default): Fast compression
                    - `zstd-{1-22}`: Zstandard with configurable level
                  '';
                };
              };
            });
            default = null;
            description = "Filesystem options.";
          };

          servers = lib.mkOption {
            type = lib.types.submodule {
              options = {
                nfs = lib.mkOption {
                  type = lib.types.nullOr (lib.types.submodule {
                    options.addresses = lib.mkOption {
                      type = lib.types.listOf lib.types.str;
                      default = [ "127.0.0.1:2049" ];
                      description = "NFS server bind addresses.";
                    };
                  });
                  default = null;
                  description = "NFS server configuration.";
                };

                ninep = lib.mkOption {
                  type = lib.types.nullOr (lib.types.submodule {
                    options = {
                      addresses = lib.mkOption {
                        type = lib.types.nullOr (lib.types.listOf lib.types.str);
                        default = null;
                        description = "9P server bind addresses.";
                      };

                      unix_socket = lib.mkOption {
                        type = lib.types.nullOr lib.types.str;
                        default = null;
                        description = "Unix socket path for 9P server.";
                      };
                    };
                  });
                  default = null;
                  description = "9P server configuration.";
                };

                nbd = lib.mkOption {
                  type = lib.types.nullOr (lib.types.submodule {
                    options = {
                      addresses = lib.mkOption {
                        type = lib.types.nullOr (lib.types.listOf lib.types.str);
                        default = null;
                        description = "NBD server bind addresses.";
                      };

                      unix_socket = lib.mkOption {
                        type = lib.types.nullOr lib.types.str;
                        default = null;
                        description = "Unix socket path for NBD server.";
                      };
                    };
                  });
                  default = null;
                  description = "NBD server configuration.";
                };

                rpc = lib.mkOption {
                  type = lib.types.nullOr (lib.types.submodule {
                    options = {
                      addresses = lib.mkOption {
                        type = lib.types.nullOr (lib.types.listOf lib.types.str);
                        default = null;
                        description = "RPC server bind addresses.";
                      };

                      unix_socket = lib.mkOption {
                        type = lib.types.nullOr lib.types.str;
                        default = null;
                        description = "Unix socket path for RPC server.";
                      };
                    };
                  });
                  default = null;
                  description = "RPC server configuration for admin commands.";
                };
              };
            };
            default = { };
            description = "Server configurations for NFS, 9P, NBD, and RPC.";
          };

          lsm = lib.mkOption {
            type = lib.types.nullOr (lib.types.submodule {
              options = {
                l0_max_ssts = lib.mkOption {
                  type = lib.types.nullOr lib.types.ints.positive;
                  default = null;
                  description = "Max SST files in L0 before compaction (default: 16, min: 4).";
                };

                max_unflushed_gb = lib.mkOption {
                  type = lib.types.nullOr lib.types.numbers.positive;
                  default = null;
                  description = "Max unflushed data before forcing flush in GB (default: 1.0).";
                };

                max_concurrent_compactions = lib.mkOption {
                  type = lib.types.nullOr lib.types.ints.positive;
                  default = null;
                  description = "Max concurrent compaction operations (default: 8).";
                };

                flush_interval_secs = lib.mkOption {
                  type = lib.types.nullOr lib.types.ints.positive;
                  default = null;
                  description = "Interval between periodic flushes in seconds (default: 30).";
                };
              };
            });
            default = null;
            description = "LSM tree performance tuning options.";
          };
        };
      };
      default = { };
      description = ''
        Configuration for ZeroFS in TOML format. See
        <https://www.zerofs.net/configuration> for available options.

        Cloud provider credentials (aws, azure, gcp sections) can be added
        using freeform attributes. Example:

        ```nix
        services.zerofs.settings = {
          aws = {
            access_key_id = "''${AWS_ACCESS_KEY_ID}";
            secret_access_key = "''${AWS_SECRET_ACCESS_KEY}";
            region = "us-east-1";
          };
        };
        ```
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.zerofs = {
      description = "ZeroFS S3 Filesystem";
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      serviceConfig = {
        Type = "simple";
        ExecStartPre = lib.optionals (socketDirs != []) [
          "+${pkgs.coreutils}/bin/mkdir -p ${lib.escapeShellArgs socketDirs}"
        ];
        ExecStart =
          "${cfg.package}/bin/zerofs run --config ${configFile}"
          + lib.optionalString cfg.readOnly " --read-only";
        Restart = "always";
        RestartSec = 5;
      } // lib.optionalAttrs (cfg.environmentFile != null) {
        EnvironmentFile = cfg.environmentFile;
      };
    };
  };
}
