{ inputs, ... }:
{
  flake.nixosModules = {
    zerofs = import ../nixos/zerofs.nix;
    default = inputs.self.nixosModules.zerofs;
  };
}
