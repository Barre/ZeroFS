{ inputs, ... }:
{
  flake.overlays = {
    zerofs = final: prev: {
      zerofs = final.callPackage "${inputs.self}/nix/package.nix" {
        craneLib = inputs.crane.mkLib final;
      };
    };

    default = inputs.self.overlays.zerofs;
  };
}
