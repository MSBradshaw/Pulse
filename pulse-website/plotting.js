document.getElementById("plot_button").addEventListener("click", function(e){
	e.preventDefault();
	var search_terms = document.getElementById("search_terms").value.replace(/, /g,',').split(',')

	var terms = '';
	var i;
	for (i = 0; i < search_terms.length; i++) {
		if(i ==0){
			terms += '"' + search_terms[i] + '"'
		}else{
			terms += ',"' + search_terms[i] + '"'
		}
	}
	var data = JSON.stringify({ "terms": search_terms, "from_date": "","to_date": "" });

	// send a POST request for the data
	var xhr = new XMLHttpRequest();
	xhr.open("POST", "https://q2whi6ss38.execute-api.us-east-2.amazonaws.com/prod/transactions", true);
	xhr.setRequestHeader("Content-Type", "application/json");
	xhr.onreadystatechange = function () {
		console.log('Recieved Info!!')
		var resp = JSON.parse(xhr.responseText);
		console.log('----')
		console.log(xhr)
		console.log(resp)
		console.log('----')
		Plotly.newPlot('plot', resp);
	};
	console.log({ "terms": search_terms, "from_date": "","to_date": "" })
	xhr.send(data);

});
