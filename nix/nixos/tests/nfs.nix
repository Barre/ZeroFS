{ pkgs, ... }:
{
  name = "nfs";

  nodes.machine = { pkgs, ... }: {
    # Minio for S3-compatible storage
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
        servers.nfs.addresses = [ "127.0.0.1:2049" ];
        aws = {
          access_key_id = "minioadmin";
          secret_access_key = "minioadmin";
          endpoint = "http://127.0.0.1:9000";
          region = "us-east-1";
          allow_http = "true";
        };
      };
    };

    # NFS client for mounting
    environment.systemPackages = [ pkgs.nfs-utils ];
  };

  testScript = ''
    machine.wait_for_unit("minio.service")
    machine.wait_for_open_port(9000)

    # Create the bucket for zerofs
    machine.succeed(
      "${pkgs.minio-client}/bin/mc alias set local http://127.0.0.1:9000 minioadmin minioadmin",
      "${pkgs.minio-client}/bin/mc mb local/zerofs-test",
    )

    machine.wait_for_unit("zerofs.service")

    # Wait for NFS to be ready
    machine.wait_for_open_port(2049)

    # Mount zerofs via NFS
    machine.succeed("mkdir -p /mnt/zerofs")
    machine.succeed("mount -t nfs -o async,nolock,rsize=1048576,wsize=1048576,tcp,port=2049,mountport=2049,hard 127.0.0.1:/ /mnt/zerofs")

    # Test basic file operations
    machine.succeed("echo 'Hello from ZeroFS!' > /mnt/zerofs/test.txt")
    output = machine.succeed("cat /mnt/zerofs/test.txt")
    assert "Hello from ZeroFS!" in output, f"Unexpected content: {output}"

    # Test file listing
    machine.succeed("ls -la /mnt/zerofs/")

    # Test file deletion
    machine.succeed("rm /mnt/zerofs/test.txt")
    machine.succeed("test ! -f /mnt/zerofs/test.txt")

    machine.succeed("umount /mnt/zerofs")
  '';
}
