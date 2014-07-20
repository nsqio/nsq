package templates

func init() {
	registerTemplate("js.html", `
<script src="/static/jquery-1.8.2.min.js"></script>
<script src="/static/twitter-bootstrap/2.2.1/js/bootstrap.min.js"></script>
<script>
$('.dropdown-toggle').dropdown()

/**
 * bootbox.js v3.0.0
 *
 * http://bootboxjs.com/license.txt
 */
var bootbox=window.bootbox||function(v,n){function h(b,a){null==a&&(a=r);return"string"===typeof l[a][b]?l[a][b]:a!=s?h(b,s):b}var r="en",s="en",t=!0,q="static",u="",g={},m={setLocale:function(b){for(var a in l)if(a==b){r=b;return}throw Error("Invalid locale: "+b);},addLocale:function(b,a){"undefined"===typeof l[b]&&(l[b]={});for(var c in a)l[b][c]=a[c]},setIcons:function(b){g=b;if("object"!==typeof g||null==g)g={}},alert:function(){var b="",a=h("OK"),c=null;switch(arguments.length){case 1:b=arguments[0];
break;case 2:b=arguments[0];"function"==typeof arguments[1]?c=arguments[1]:a=arguments[1];break;case 3:b=arguments[0];a=arguments[1];c=arguments[2];break;default:throw Error("Incorrect number of arguments: expected 1-3");}return m.dialog(b,{label:a,icon:g.OK,callback:c},{onEscape:c||!0})},confirm:function(){var b="",a=h("CANCEL"),c=h("CONFIRM"),e=null;switch(arguments.length){case 1:b=arguments[0];break;case 2:b=arguments[0];"function"==typeof arguments[1]?e=arguments[1]:a=arguments[1];break;case 3:b=
arguments[0];a=arguments[1];"function"==typeof arguments[2]?e=arguments[2]:c=arguments[2];break;case 4:b=arguments[0];a=arguments[1];c=arguments[2];e=arguments[3];break;default:throw Error("Incorrect number of arguments: expected 1-4");}var j=function(){"function"===typeof e&&e(!1)};return m.dialog(b,[{label:a,icon:g.CANCEL,callback:j},{label:c,icon:g.CONFIRM,callback:function(){"function"===typeof e&&e(!0)}}],{onEscape:j})},prompt:function(){var b="",a=h("CANCEL"),c=h("CONFIRM"),e=null,j="";switch(arguments.length){case 1:b=
arguments[0];break;case 2:b=arguments[0];"function"==typeof arguments[1]?e=arguments[1]:a=arguments[1];break;case 3:b=arguments[0];a=arguments[1];"function"==typeof arguments[2]?e=arguments[2]:c=arguments[2];break;case 4:b=arguments[0];a=arguments[1];c=arguments[2];e=arguments[3];break;case 5:b=arguments[0];a=arguments[1];c=arguments[2];e=arguments[3];j=arguments[4];break;default:throw Error("Incorrect number of arguments: expected 1-5");}var d=n("<form></form>");d.append("<input autocomplete=off type=text value='"+
j+"' />");var j=function(){"function"===typeof e&&e(null)},k=m.dialog(d,[{label:a,icon:g.CANCEL,callback:j},{label:c,icon:g.CONFIRM,callback:function(){"function"===typeof e&&e(d.find("input[type=text]").val())}}],{header:b,show:!1,onEscape:j});k.on("shown",function(){d.find("input[type=text]").focus();d.on("submit",function(a){a.preventDefault();k.find(".btn-primary").click()})});k.modal("show");return k},dialog:function(b,a,c){var e="",j=[];c=c||{};null==a?a=[]:"undefined"==typeof a.length&&(a=
[a]);for(var d=a.length;d--;){var k=null,g=null,h=null,l="",m=null;if("undefined"==typeof a[d].label&&"undefined"==typeof a[d]["class"]&&"undefined"==typeof a[d].callback){var k=0,g=null,p;for(p in a[d])if(g=p,1<++k)break;1==k&&"function"==typeof a[d][p]&&(a[d].label=g,a[d].callback=a[d][p])}"function"==typeof a[d].callback&&(m=a[d].callback);a[d]["class"]?h=a[d]["class"]:d==a.length-1&&2>=a.length&&(h="btn-primary");k=a[d].label?a[d].label:"Option "+(d+1);a[d].icon&&(l="<i class='"+a[d].icon+"'></i> ");
g=a[d].href?a[d].href:"javascript:;";e="<a data-handler='"+d+"' class='btn "+h+"' href='"+g+"'>"+l+""+k+"</a>"+e;j[d]=m}d=["<div class='bootbox modal' tabindex='-1' style='overflow:hidden;'>"];if(c.header){h="";if("undefined"==typeof c.headerCloseButton||c.headerCloseButton)h="<a href='javascript:;' class='close'>&times;</a>";d.push("<div class='modal-header'>"+h+"<h3>"+c.header+"</h3></div>")}d.push("<div class='modal-body'></div>");e&&d.push("<div class='modal-footer'>"+e+"</div>");d.push("</div>");
var f=n(d.join("\n"));("undefined"===typeof c.animate?t:c.animate)&&f.addClass("fade");(e="undefined"===typeof c.classes?u:c.classes)&&f.addClass(e);f.find(".modal-body").html(b);f.on("hidden",function(){f.remove()});f.on("keyup.dismiss.modal",function(a){if(27==a.which&&c.onEscape){if("function"===typeof c.onEscape)c.onEscape();f.modal("hide")}});f.on("shown",function(){f.find("a.btn-primary:first").focus()});f.on("click",".modal-footer a, a.close",function(b){var c=n(this).data("handler"),d=j[c],
e=null;"undefined"!==typeof c&&"undefined"!==typeof a[c].href||(b.preventDefault(),"function"==typeof d&&(e=d()),!1!==e&&f.modal("hide"))});n("body").append(f);f.modal({backdrop:"undefined"===typeof c.backdrop?q:c.backdrop,keyboard:!1,show:!1});f.on("show",function(){n(v).off("focusin.modal")});("undefined"===typeof c.show||!0===c.show)&&f.modal("show");return f},modal:function(){var b,a,c,e={onEscape:null,keyboard:!0,backdrop:q};switch(arguments.length){case 1:b=arguments[0];break;case 2:b=arguments[0];
"object"==typeof arguments[1]?c=arguments[1]:a=arguments[1];break;case 3:b=arguments[0];a=arguments[1];c=arguments[2];break;default:throw Error("Incorrect number of arguments: expected 1-3");}e.header=a;c="object"==typeof c?n.extend(e,c):e;return m.dialog(b,[],c)},hideAll:function(){n(".bootbox").modal("hide")},animate:function(b){t=b},backdrop:function(b){q=b},classes:function(b){u=b}},l={en:{OK:"OK",CANCEL:"Cancel",CONFIRM:"OK"},fr:{OK:"OK",CANCEL:"Annuler",CONFIRM:"D'accord"},de:{OK:"OK",CANCEL:"Abbrechen",
CONFIRM:"Akzeptieren"},es:{OK:"OK",CANCEL:"Cancelar",CONFIRM:"Aceptar"},br:{OK:"OK",CANCEL:"Cancelar",CONFIRM:"Sim"},nl:{OK:"OK",CANCEL:"Annuleren",CONFIRM:"Accepteren"},ru:{OK:"OK",CANCEL:"\u041e\u0442\u043c\u0435\u043d\u0430",CONFIRM:"\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c"},it:{OK:"OK",CANCEL:"Annulla",CONFIRM:"Conferma"}};return m}(document,window.jQuery);window.bootbox=bootbox;

$("form").submit(function(ev) {
    bootbox.confirm("Are you sure?", function(result) {
        if (result) {
            ev.target.submit();
        }
    });
    return false;
});

$('.rate').each(function()  {
    var self = this;
    $.getJSON("/graphite_data", {
      metric: "rate",
      target: $(this).attr("target")
    },
    function(rate) {
        $(self).html(rate.datapoint);
    });
});

$('.conn-counts').each(function() {
    $(this).on('click', function(ev) {
        ev.preventDefault();
        $(this).next().toggle();
    });
});
</script>
`)
}
