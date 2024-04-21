$(document).ready(function() {
    $('#dataForm').submit(function(event) {
        event.preventDefault();

        var formData = new FormData();
        var inputFile = $('#inputFile')[0].files[0];
        formData.append('file', inputFile);

        $.ajax({
            type: 'POST',
            url: '/process_data',  // Backend endpoint for data processing
            data: formData,
            processData: false,
            contentType: false,
            success: function(response) {
                $('#processedData').html(response);
            },
            error: function(xhr, status, error) {
                console.error(error);
            }
        });
    });
});
