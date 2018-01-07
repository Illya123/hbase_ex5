/**
 * Created by illya on 07.01.18.
 */
public class ActorInfo
{
    private String actorName;
    private String film;
    private String role;

    ActorInfo(String actorName, String film, String role)
    {
        this.actorName = actorName;
        this.film = film;
        this.role = role;
    }

    public String getActorName()
    {
        if(this.actorName == null)
        {
            return "unnamed";
        }
        return actorName;
    }

    public void setActorName(String actorName)
    {
        this.actorName = actorName;
    }

    public String getFilm()
    {
        if(this.film == null)
        {
            return "unnamed";
        }
        return film;
    }

    public void setFilm(String film)
    {
        this.film = film;
    }

    public String getRole()
    {
        if(this.role == null)
        {
            return "unnamed";
        }
        return role;
    }

    public void setRole(String role)
    {
        this.role = role;
    }
}
