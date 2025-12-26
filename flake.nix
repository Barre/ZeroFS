{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };

    crane.url = "github:ipetkov/crane";
  };

  outputs = inputs @ { self, flake-parts, ... }: let
    inherit (inputs.nixpkgs) lib;

    modules = builtins.foldl' (acc: f: f acc) ./nix/flake [
      builtins.readDir
      (lib.filterAttrs (name: type:
        type == "regular" && lib.hasSuffix ".nix" name
      ))
      (lib.mapAttrsToList (name: _:
        lib.path.append ./nix/flake name
      ))
    ];

  in flake-parts.lib.mkFlake { inherit inputs; } {
    imports = modules;
    systems = [
      "x86_64-linux"
      "aarch64-linux"
    ];
  };
}
