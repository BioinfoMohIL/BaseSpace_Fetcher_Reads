version 1.0


task version_capture {
  input {
    String? timezone
    String docker = "us-docker.pkg.dev/general-theiagen/theiagen/alpine-plus-bash:3.20.0"
  }
  meta {
    volatile: true
  }
  command {
    PHB_Version="PHB v2.3.0"
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
    Int cpu = 2
    Int disk_size = 100

    String docker = "us-docker.pkg.dev/general-theiagen/theiagen/basespace_cli:1.2.1"
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

    # rename FASTQ files to add back in underscores that Illumina/Basespace changed into hyphens
    echo "Concatenating and renaming FASTQ files to add back underscores in basespace_sample_name"
    # setting a new bash variable to use for renaming during concatenation of FASTQs
    SAMPLENAME_HYPHEN_INSTEAD_OF_UNDERSCORES=$(echo $sample_identifier | sed 's|_|-|g' | sed 's|\.|-|g')

    #Combine non-empty read files into single file without BaseSpace filename cruft
    ##FWD Read
    echo -n "" > fwd_sizes.txt
    echo -n "" > rev_sizes.txt
    lane_count=0
    for fwd_read in ./dataset_*/${SAMPLENAME_HYPHEN_INSTEAD_OF_UNDERSCORES}_*R1_*.fastq.gz; do
      if [[ -s $fwd_read ]]; then
        fwd_read_name=$(basename "$fwd_read") # Fetch the complete name
        echo $fwd_read_name > fwd_read_name.txt

        fwd_file_size=$(stat -c%s "$fwd_read")
        echo $fwd_file_size > fwd_size.txt
        
        echo "cat fwd reads: cat $fwd_read >> $fwd_read_name" 
        cat $fwd_read >> $fwd_read_name
        lane_count=$((lane_count+1))
      fi
    done
    ##REV Read
    for rev_read in ./dataset_*/${SAMPLENAME_HYPHEN_INSTEAD_OF_UNDERSCORES}_*R2_*.fastq.gz; do
      if [[ -s $rev_read ]]; then 
        rev_read_name=$(basename "$rev_read")   # Fetch the complete name
        echo $rev_read_name > rev_read_name.txt
        
        rev_file_size=$(stat -c%s "$rev_read")
        echo $rev_file_size > rev_size.txt
        
        echo "cat rev reads: cat $rev_read >> $rev_read_name" 
        cat $rev_read >> $rev_read_name
      fi
    done
    echo "Lane Count: ${lane_count}"

  >>>
  output {
    File read1  = read_string("fwd_read_name.txt")
    File? read2 = read_string("rev_read_name.txt")
    String fwd_file_size = read_string("fwd_size.txt")
    String rev_file_size = read_string("rev_size.txt")

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

    String read1_file_size = fetch_bs.fwd_file_size
    String read2_file_size = fetch_bs.rev_file_size
  }
}
