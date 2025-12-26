{ pkgs, ... }:
{
  name = "9p-tcp";

  nodes.machine = { pkgs, ... }: {
    services.minio = {
      enable = true;
      rootCredentialsFile = pkgs.writeText "minio-credentials" ''
        MINIO_ROOT_USER=minioadmin
        MINIO_ROOT_PASSWORD=minioadmin
      '';
    };

    services.zerofs = {
      enable = true;
      settings = {
        cache = {
          dir = "/var/cache/zerofs";
          disk_size_gb = 1.0;
        };
        storage = {
          url = "s3://zerofs-test/data";
          encryption_password = "test-password";
        };
        servers.ninep.addresses = [ "127.0.0.1:5564" ];
        aws = {
          access_key_id = "minioadmin";
          secret_access_key = "minioadmin";
          endpoint = "http://127.0.0.1:9000";
          region = "us-east-1";
          allow_http = "true";
        };
      };
    };
  };

  testScript = ''
    machine.wait_for_unit("minio.service")
    machine.wait_for_open_port(9000)

    machine.succeed(
      "${pkgs.minio-client}/bin/mc alias set local http://127.0.0.1:9000 minioadmin minioadmin",
      "${pkgs.minio-client}/bin/mc mb local/zerofs-test",
    )

    machine.wait_for_unit("zerofs.service")
    machine.wait_for_open_port(5564)

    # Mount zerofs via 9P over TCP
    machine.succeed("mkdir -p /mnt/zerofs")
    machine.succeed("mount -t 9p -o trans=tcp,port=5564,version=9p2000.L,cache=mmap 127.0.0.1 /mnt/zerofs")

    # Test basic file operations
    machine.succeed("echo 'Hello from ZeroFS 9P!' > /mnt/zerofs/test.txt")
    output = machine.succeed("cat /mnt/zerofs/test.txt")
    assert "Hello from ZeroFS 9P!" in output, f"Unexpected content: {output}"

    machine.succeed("ls -la /mnt/zerofs/")

    machine.succeed("rm /mnt/zerofs/test.txt")
    machine.succeed("test ! -f /mnt/zerofs/test.txt")

    machine.succeed("umount /mnt/zerofs")
  '';
}
