{ inputs, lib, ... }:
{
  perSystem =
    { pkgs, system, ... }:
    let
      mkTest =
        name:
        lib.nixos.evalTest {
          imports = [ (../nixos/tests + "/${name}.nix") ];
          hostPkgs = pkgs;
          defaults = {
            imports = [ inputs.self.nixosModules.default ];
            services.zerofs.package = lib.mkDefault inputs.self.packages.${system}.default;
          };
        };

      tests = {
        nfs = mkTest "nfs";
        "9p-tcp" = mkTest "9p-tcp";
        "9p-uds" = mkTest "9p-uds";
        "nbd-zfs" = mkTest "nbd-zfs";
      };
    in
    {
      apps = lib.mapAttrs' (
        name: test:
        lib.nameValuePair "test-${name}" {
          type = "app";
          program = "${test.config.result.driver}/bin/nixos-test-driver";
        }
      ) tests;
    };
}
