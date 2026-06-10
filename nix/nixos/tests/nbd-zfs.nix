{ pkgs, ... }:
{
  name = "nbd-zfs";

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
          disk_size_gb = 2.0;
          memory_size_gb = 2.0;
        };
        storage = {
          url = "s3://zerofs-test/data";
          encryption_password = "test-password";
        };
        servers.ninep.addresses = [ "127.0.0.1:5564" ];
        servers.nbd.addresses = [ "127.0.0.1:10809" ];
        # Higher L0 limits needed for slow VM environments where compaction can't keep up
        lsm = {
          l0_max_ssts = 64;
          max_concurrent_compactions = 32;
        };
        aws = {
          access_key_id = "minioadmin";
          secret_access_key = "minioadmin";
          endpoint = "http://127.0.0.1:9000";
          region = "us-east-1";
          allow_http = "true";
        };
      };
    };

    boot.supportedFilesystems = [ "zfs" ];
    boot.zfs.forceImportRoot = false;
    boot.kernelModules = [ "nbd" ];
    networking.hostId = "deadbeef";

    environment.systemPackages = with pkgs; [
      nbd
      zfs
    ];
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
    machine.wait_for_open_port(10809)

    # Mount 9P and create NBD device file
    machine.succeed("mkdir -p /mnt/9p")
    machine.succeed("mount -t 9p -o trans=tcp,port=5564,version=9p2000.L 127.0.0.1 /mnt/9p")
    machine.succeed("mkdir -p /mnt/9p/.nbd")
    machine.succeed("truncate -s 1G /mnt/9p/.nbd/test-device")
    machine.succeed("umount /mnt/9p")

    # Connect NBD device
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N test-device")
    machine.succeed("blockdev --getsize64 /dev/nbd0")

    # Create ZFS pool and filesystem
    machine.succeed("zpool create testpool /dev/nbd0")
    machine.succeed("zfs create testpool/data")
    machine.succeed("zfs set mountpoint=/mnt/zfsdata testpool/data")

    # Write test data
    machine.succeed("echo 'Hello from ZeroFS NBD+ZFS!' > /mnt/zfsdata/test.txt")
    machine.succeed("dd if=/dev/urandom of=/mnt/zfsdata/random.dat bs=1M count=10")
    original_checksum = machine.succeed("sha256sum /mnt/zfsdata/random.dat").strip()

    # Create snapshot
    machine.succeed("zfs snapshot testpool/data@snap1")

    # Sync and export
    machine.succeed("sync")
    machine.succeed("zpool sync testpool")
    machine.succeed("zfs unmount testpool/data")
    machine.succeed("zpool export testpool")

    # Disconnect NBD and wait for full disconnect
    machine.succeed("nbd-client -d /dev/nbd0")
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")
    machine.wait_until_succeeds("! nbd-client -c /dev/nbd0")

    # Restart zerofs
    machine.succeed("systemctl stop zerofs.service")
    machine.succeed("systemctl start zerofs.service")
    machine.wait_for_unit("zerofs.service")
    machine.wait_for_open_port(10809)

    # Reconnect NBD and import pool
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N test-device")
    machine.succeed("zpool import testpool")

    # Verify data integrity
    output = machine.succeed("cat /mnt/zfsdata/test.txt")
    assert "Hello from ZeroFS NBD+ZFS!" in output, f"Unexpected content: {output}"

    restored_checksum = machine.succeed("sha256sum /mnt/zfsdata/random.dat").strip()
    assert original_checksum == restored_checksum, f"Checksum mismatch: {original_checksum} vs {restored_checksum}"

    # Verify snapshot exists
    machine.succeed("zfs list -t snapshot testpool/data@snap1")

    # Cleanup
    machine.succeed("zpool export testpool")
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
