version 1.0

task GetReads {
    input {
        String basespace_sample_name
        String? basespace_sample_id   
        String basespace_collection_id
        String api_server 
        String access_token
        String docker = "us-docker.pkg.dev/general-theiagen/theiagen/basespace_cli:1.2.1"

    }

    command <<<
        read1=""
        read2=""

        bs project content --name N_019 \
            --api-server=~{api_server} \
            --access-token=~{access_token} \
            --retry > fastq_list.txt

        read1=$(grep "${sample_name}.*_R1_" fastq_list.txt | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' | head -n 1)
        read2=$(grep "${sample_name}.*_R2_" fastq_list.txt | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/, "", $3); print $3}' | head -n 1)

        echo $read1 > 'read1.txt' 
        echo $read2 > 'read2.txt' 
    >>>

    output {
        String read1_whole_name = read_string('read1.txt') 
        String read2_whole_name = read_string('read2.txt')  
    }

    runtime {
        docker: docker
        preemptible: 1
  }
}

workflow FetchReads {
    input {
        String basespace_sample_name 
        String basespace_collection_id
        String api_server
        String access_token

    }

    call GetReads {
        input:
            basespace_collection_id = basespace_collection_id,
            access_token = access_token,
            api_server = api_server,
            basespace_sample_name = basespace_sample_name
    }

    output {
        String read1_name = GetReads.read1_whole_name
        String read2_name = GetReads.read2_whole_name
    }

  
}
