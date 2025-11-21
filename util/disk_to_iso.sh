#!/bin/bash
set -euo pipefail

# disk_to_iso.sh
# Usage: ./disk_to_iso.sh sda
# Creates a new script named disk_<device>_to_iso.sh which contains
# a single command to list partitions for /dev/<device> using sudo fdisk -l

# parse options: support -f/--force to create target_dir if missing
FORCE=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [-f|--force] <device_filename> <target_dir>" >&2
      echo "Example: $0 -f sda /tmp" >&2
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 [-f|--force] <device_filename> <target_dir>" >&2
  echo "Example: $0 -f sda /tmp" >&2
  exit 2
fi

device="$1"
target_dir="$2"

# Basic validation: allow letters, digits, dash and underscore for device
if [[ ! "$device" =~ ^[A-Za-z0-9_\-]+$ ]]; then
  echo "Invalid device name: '$device'" >&2
  exit 3
fi

# Validate target directory exists, is a directory, and is writable. If missing and -f given, attempt to create it.
if [ ! -e "$target_dir" ]; then
  if [ "$FORCE" -eq 1 ]; then
    if ! mkdir -p -- "$target_dir"; then
      echo "Failed to create target directory '$target_dir'" >&2
      exit 4
    fi
  else
    echo "Target path does not exist: $target_dir" >&2
    exit 4
  fi
fi
if [ ! -d "$target_dir" ]; then
  echo "Target path is not a directory: $target_dir" >&2
  exit 5
fi
if [ ! -w "$target_dir" ]; then
  echo "Target directory is not writable: $target_dir" >&2
  exit 6
fi

out_file="disk_${device}_to_iso.sh"
out_geo_file="${target_dir%/}/disk_${device}_geometry.txt"

# Early check: verify we can run fdisk -l on the device (requires privileges on many systems).
# If the check fails, prompt the user to re-run the generator with elevated privileges.
if ! fdisk -l "/dev/$device" >/dev/null 2>&1; then
  echo "fdisk probe failed for /dev/$device. This operation usually requires root privileges."
  echo "Please re-run this script with sudo or as root, for example: sudo $0 $device $target_dir" >&2
  exit 7
fi

# Probe device partitions using fdisk and build per-partition dd commands.
# We exclude any partition lines that indicate an Extended partition.
part_list=()
# Use fdisk to list partitions; filter lines starting with /dev/ but not the header that ends with ':'
if fdisk -l "/dev/$device" >/dev/null 2>&1; then
  # Read partition device names into an array
  mapfile -t part_list < <(fdisk -l "/dev/$device" 2>/dev/null | grep '^/dev/' | grep -v ':$' | grep -vi extended | awk '{print $1}')
else
  echo "Warning: fdisk failed to probe /dev/$device; generated script will still contain the fdisk listing command." >&2
fi

# Write the new script: fdisk output redirection plus dd commands per partition
{
  # safety flags in generated script: fail fast on any error and detect pipeline failures
  printf "#!/bin/bash\n"
  printf "set -euo pipefail\n"
  printf "trap 'echo \"Script failed on line \$LINENO\" >&2; exit 1' ERR\n\n"
  printf "\n"
  printf "# Generated script to capture disk geometry and partition images for /dev/%s\n" "$device"
  printf "echo \"(disk geometry file being created: %s)\"\n" "$out_geo_file"
  # explicit check for fdisk success
  printf "\n"
  printf "if ! sudo fdisk -l /dev/%s > %s; then echo \"fdisk failed for /dev/%s\" >&2; exit 1; fi\n" "$device" "$out_geo_file" "$device"
  # Create a dd command to capture bytes from offset 0 up to the first partition start
  printf "\n"
  printf "# Create image of disk area from byte 0 up to the first partition start (assumes 512B sectors)\n"
  printf "min_start=\$(fdisk -l /dev/%s 2>/dev/null | awk '/^\\/dev\\//{ if(\$2+0>0) print \$2 }' | sort -n | head -n1)\n" "$device"
  printf "\n"
  printf "if [ -n \"\$min_start\" ] && [ \"\$min_start\" -gt 0 ]; then\n"
  # compute the pre-partition output filename now so we can embed it literally
  outer_out_pre_img="${target_dir%/}/diskimage_${device}_prepartition.iso.gz"
  printf "  out_pre_img=\"%s\"\n" "$outer_out_pre_img"
  printf "  echo \"Imaging disk pre-partition area (sectors 0..\$min_start-1) -> %s\"\n" "$outer_out_pre_img"
  printf "  if ! sudo dd if=/dev/%s bs=512 count=\$min_start status=progress conv=sync,noerror | gzip -c > \"\$out_pre_img\"; then echo \"dd failed for pre-partition area\" >&2; exit 1; fi\n" "$device"
  printf "fi\n"
  if [ ${#part_list[@]} -eq 0 ]; then
    printf "# No partitions found or fdisk parse failed; no dd commands added.\n"
  else
    for p in "${part_list[@]}"; do
      # basename of partition (e.g., sda1 or nvme0n1p1)
      pf=$(basename "$p")
      out_img="${target_dir%/}/diskimage_${device}_${pf}.iso.gz"
      # dd piped into gzip, write to per-partition output; explicit failure check
      printf "\n"
      printf "echo \"Imaging partition %s -> %s\"\n" "$p" "$out_img"
      printf "if ! sudo dd if=%s bs=4M status=progress conv=sync,noerror | gzip -c > %s; then echo \"dd failed for %s\" >&2; exit 1; fi\n" "$p" "$out_img" "$p"
    done
  fi
} > "$out_file"

chmod +x "$out_file" || true

echo "Created $out_file"
