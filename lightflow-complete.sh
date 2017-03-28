_lightflow_completion() {
    COMPREPLY=( $( env COMP_WORDS="${COMP_WORDS[*]}" \
                   COMP_CWORD=$COMP_CWORD \
                   _LIGHTFLOW_COMPLETE=complete $1 ) )
    return 0
}

complete -F _lightflow_completion -o default lightflow;
