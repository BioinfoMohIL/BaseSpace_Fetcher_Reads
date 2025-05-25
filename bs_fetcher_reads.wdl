version 1.0

workflow basespace_fetch {
  input {
    String sample_name
    String basespace_sample_name
    String? basespace_sample_id
    String basespace_collection_id
    String api_server
    String access_token
  }

  call fetch_bs {
    input:
      sample_name = sample_name,
      basespace_sample_id = basespace_sample_id,
      basespace_sample_name = basespace_sample_name,
      basespace_collection_id = basespace_collection_id,
      api_server = api_server,
      access_token = access_token
  }

  call version_capture {
    input:
  }

  output {
    String basespace_fetch_version = version_capture.phb_version
    String basespace_fetch_analysis_date = version_capture.date
    
    File read1 = fetch_bs.read1
    File? read2 = fetch_bs.read2

    Float read1_file_size_mb = fetch_bs.fwd_file_size
    Float read2_file_size_mb = fetch_bs.rev_file_size

    Float bs_read1_file_size_mb = fetch_bs.bs_fwd_file_size
    Float bs_read2_file_size_mb = fetch_bs.bs_rev_file_size
  }
}


task version_capture {
  input {
    String? timezone
    String docker = "us-docker.pkg.dev/general-theiagen/theiagen/alpine-plus-bash:3.20.0"
  }
  meta {
    volatile: true
  }
  command {
    PHB_Version="PHB v3.0.1"
    ~{default='' 'export TZ=' + timezone}
    date +"%Y-%m-%d" > TODAY
    echo "$PHB_Version" > PHB_VERSION
  }
  output {
    String date = read_string("TODAY")
    String phb_version = read_string("PHB_VERSION")
  }
  runtime {
    memory: "1 GB"
    cpu: 1
    docker: docker
    disks: "local-disk 10 HDD"
    dx_instance_type: "mem1_ssd1_v2_x2" 
    preemptible: 1
  }
}

task fetch_bs {
  input {
    String sample_name
    String basespace_sample_name
    String? basespace_sample_id
    String basespace_collection_id
    String api_server
    String access_token
    
    Int memory = 8
    Int cpu = 4
    Int disk_size = 100

    # String docker = "us-docker.pkg.dev/general-theiagen/theiagen/basespace_cli:1.2.1"
    String docker = "bioinfomoh/basespace-cli:1.6.2"
  }
  meta {
    # added so that call caching is always turned off
    volatile: true
  }
  command <<<
    # set basespace name and id variables
    if [[ ! -z "~{basespace_sample_id}" ]]; then
      sample_identifier="~{basespace_sample_name}"
      dataset_name="~{basespace_sample_id}"
    else
      sample_identifier="~{basespace_sample_name}"
      dataset_name="~{basespace_sample_name}"
    fi
    
    # print all relevant input variables to stdout
    echo -e "sample_identifier: ${sample_identifier}\ndataset_name: ${dataset_name}\nbasespace_collection_id: ~{basespace_collection_id}"
      
    #Set BaseSpace comand prefix
    bs_command="bs --api-server=~{api_server} --access-token=~{access_token}"
    echo "bs_command: ${bs_command}"

    #Grab BaseSpace Run_ID from given BaseSpace Run Name
    run_id=$(${bs_command} list run --retry | grep "~{basespace_collection_id}" | awk -F "|" '{ print $3 }' | awk '{$1=$1;print}' )
    echo "run_id: ${run_id}" 
    if [[ ! -z "${run_id}" ]]; then 
      #Grab BaseSpace Dataset ID from dataset lists within given run 
      dataset_id_array=($(${bs_command} list dataset --retry --input-run=${run_id} | grep "${dataset_name}" | awk -F "|" '{ print $3 }' )) 
      echo "dataset_id: ${dataset_id_array[*]}"
    else 
      #Try Grabbing BaseSpace Dataset ID from project name
      echo "Could not locate a run_id via Basespace runs, attempting to search Basespace projects now..."
      project_id=$(${bs_command} list project --retry | grep "~{basespace_collection_id}" | awk -F "|" '{ print $3 }' | awk '{$1=$1;print}' )
      echo "project_id: ${project_id}" 
      if [[ ! -z "${project_id}" ]]; then 
        echo "project_id identified via Basespace, now searching for dataset_id within project_id ${project_id}..."
        dataset_id_array=($(${bs_command} list dataset --retry --project-id=${project_id} | grep "${dataset_name}" | awk -F "|" '{ print $3 }' )) 
        echo "dataset_id: ${dataset_id_array[*]}"
      else       
        echo "No run or project id found associated with input basespace_collection_id: ~{basespace_collection_id}" >&2
        exit 1
      fi      
    fi


    #Download reads by dataset ID
    for index in ${!dataset_id_array[@]}; do
      dataset_id=${dataset_id_array[$index]}
      mkdir ./dataset_${dataset_id} && cd ./dataset_${dataset_id}
      echo "dataset download: ${bs_command} download dataset -i ${dataset_id} -o . --retry"
      ${bs_command} download dataset --retry -i ${dataset_id} -o . --retry && cd ..
      echo -e "downloaded data: \n $(ls ./dataset_*/*)"
    done

    # Fetch BS file size form CLI to compare with the actual file size fetched
    # Initialize total files if missing
    bs_r1_total_file_size='bs_read1_file_size.txt'
    bs_r2_total_file_size='bs_read2_file_size.txt'

    [ ! -s "$bs_r1_total_file_size" ] && echo "0" > "$bs_r1_total_file_size"
    [ ! -s "$bs_r2_total_file_size" ] && echo "0" > "$bs_r2_total_file_size"

    for dataset_id in "${dataset_id_array[@]}"; do

      # Fetch sizes into arrays
      r1_size_list=($(
        ${bs_command} dataset content -i "${dataset_id}" -F Size -F Name \
          | awk -F"|" '/_R1_.*fastq\.gz/ {
              gsub(/ /, "", $2);
              printf "%.2f\n", $2 / 1048576;
            }'
      ))


      r2_size_list=($(
        ${bs_command} dataset content -i "${dataset_id}" -F Size -F Name \
          | awk -F"|" '/_R2_.*fastq\.gz/ {
              gsub(/ /, "", $2);
              printf "%.2f\n", $2 / 1048576;
            }'
      ))

      # Sum R1 sizes
      r1_sum=$(awk "BEGIN {total=0; for(i in ARGV) if(i>0) total+=ARGV[i]; print total}" "${r1_size_list[@]}")
      prev_r1_total=$(cat "$bs_r1_total_file_size")
      new_r1_total=$(awk "BEGIN {printf \"%.2f\", $r1_sum + $prev_r1_total}")

      echo "$new_r1_total" > "$bs_r1_total_file_size"

      # Sum R2 sizes
      r2_sum=$(awk "BEGIN {total=0; for(i in ARGV) if(i>0) total+=ARGV[i]; print total}" "${r2_size_list[@]}")
      prev_r2_total=$(cat "$bs_r2_total_file_size")
      new_r2_total=$(awk "BEGIN {printf \"%.2f\", $r2_sum + $prev_r2_total}")
      echo "$new_r2_total" > "$bs_r2_total_file_size"

    done


    # rename FASTQ files to add back in underscores that Illumina/Basespace changed into hyphens
    echo "Concatenating and renaming FASTQ files to add back underscores in basespace_sample_name"
   
    #Combine non-empty read files into single file without BaseSpace filename cruft
    ##FWD Read
    lane_count=0

    calcul_total_size() {
      local file="$1"
      local total_file="$2"

      if [ ! -s "$total_file" ]; then
        echo "0" > "$total_file"
      fi

      local size_bytes
      size_bytes=$(stat -c%s "$file")

      # Convert to MB with two decimals
      local size_mb
      size_mb=$(awk "BEGIN {printf \"%.2f\", $size_bytes / (1024*1024)}")

      local prev_total
      prev_total=$(cat "$total_file")

      local new_total
      new_total=$(awk "BEGIN {printf \"%.2f\", $size_mb + $prev_total}")

      echo "$new_total" > "$total_file"
    }

    process_reads() {
        local sample_id="$1"       
        local sample_name="$2"     
        local read_dir="$3"       
        local total_file            # cumulative on-disk size
        local bs_total_file         # BioSample-reported size
        local lane_count=0

        total_file="${read_dir}_size.txt"        
        bs_total_file="bs_${read_dir}_size.txt"   

        [ ! -s "$total_file" ]    && echo "0" > "$total_file"
        for fq in ./dataset_*/${sample_id}_*${read_dir}_*.fastq.gz; do

          calcul_total_size "$fq" "$total_file"

          # append to the merged-fastq
          echo "cat $fq >> ${sample_name}_${read_dir}.fastq.gz"
          cat "$fq" >> "${sample_name}_${read_dir}.fastq.gz"

          lane_count=$((lane_count + 1))
        done

        echo "Processed $lane_count lanes for ${read_dir} of sample ${sample_id}."
        }

    process_reads ${sample_identifier} ${sample_identifier} "R1"
    process_reads ${sample_identifier} ${sample_identifier} "R2"


    
    echo "Lane Count: ${lane_count}"
  >>>
  output {
    File read1 = "~{sample_name}_R1.fastq.gz"
    File? read2 = "~{sample_name}_R2.fastq.gz"
    Float fwd_file_size = read_float("R1_size.txt")
    Float rev_file_size = read_float("R2_size.txt")
    Float bs_fwd_file_size = read_float("bs_read1_file_size.txt")
    Float bs_rev_file_size = read_float("bs_read2_file_size.txt")
  }

  runtime {
    docker: docker
    memory: "~{memory} GB"
    cpu: cpu
    disks: "local-disk ~{disk_size} SSD"
    disk: disk_size + " GB"
    preemptible: 1
  }
}


