
$( document ).ready(function() {

    $('.example-tab a').click(function (e) {
        e.preventDefault()
        $(this).tab('show')
    })

    $('pre').each(function(i, block) {
        hljs.highlightBlock(block);
    });

});
